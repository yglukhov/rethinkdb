import asyncdispatch, asyncnet, json, random, base64, tables, strutils, md5

import nimSHA2
import sha1, hmac
import rethinkdb.private.pbkdf2

type
    Connection* = ref object
        sock: AsyncSocket
        queryIdCounter: int64
        pendingQueries: Table[int64, Future[JsonNode]]

    QueryNode* = JsonNode
    DBNode* = JsonNode
    TableNode* = JsonNode
    SequenceNode* = JsonNode
    FunctionNode* = JsonNode
    ExpressionNode* = JsonNode

    Command {.pure.} = enum
        start = 1
        makeArray = 2

        IMPLICIT_VAR = 13

        db = 14
        table = 15
        GET = 16

        EQ  = 17
        NE  = 18
        LT  = 19
        LE  = 20
        GT  = 21
        GE  = 22
        NOT = 23

        # ADD can either add two numbers or concatenate two arrays.
        ADD = 24
        SUB = 25
        MUL = 26
        DIV = 27
        MOD = 28

        PLUCK    = 33

        filter = 39

        DELETE = 54
        insert = 56

        tableCreate = 60
        tableDrop = 61
        tableList = 62

        OR      = 66
        AND     = 67
        FUNC = 69

        BRACKET = 170


    ResponseType = enum
        SUCCESS_ATOM = 1
        SUCCESS_SEQUENCE = 2
        SUCCESS_PARTIAL = 3
        WAIT_COMPLETE = 4
        CLIENT_ERROR = 16
        COMPILE_ERROR = 17
        RUNTIME_ERROR = 18




proc readUntil(s: AsyncSocket, terminator: char): Future[string] {.async.} =
    result = ""
    var c = '\0'
    while true:
        discard await s.recvInto(addr c, sizeof(c))
        if c == terminator: break
        result &= c

proc readJson(s: AsyncSocket): Future[JsonNode] {.async.} =
    let str = await s.readUntil('\0')
    result = parseJson(str)

proc writeJson(s: AsyncSocket, j: JsonNode) {.async.} =
    var str = $j
    GC_ref(str)
    await s.send(addr str[0], str.len + 1)
    GC_unref(str)

proc checkSuccess(j: JsonNode) =
    let s = j{"success"}
    if not s.isNil and s.kind == JBool and s.bval: return
    raise newException(Exception, "Authentication error")

proc authenticate(s: AsyncSocket, username, password: string) {.async.} =
    let rand = random.random(1.0)
    let nonce = base64.encode(($rand)[2..^1])
    let fb = "n=" & username & ",r=" & nonce
    await s.writeJson(%*{
        "protocol_version": 0,
        "authentication_method": "SCRAM-SHA-256",
        "authentication": "n,," & fb
    })

    let j = await s.readJson()
    checkSuccess(j)

    let responsePayload = j["authentication"].str

    proc parsePayload(p: string): Table[string, string] =
        result = initTable[string, string]()
        for item in p.split(","):
            let e = item.find('=')
            let key = item[0..e - 1]
            let val = item[e + 1..^1]
            result[key] = val

    let parsedPayload = parsePayload(j["authentication"].str)

    let iterations = parseInt(parsedPayload["i"])
    let salt = base64.decode(parsedPayload["s"])
    let rnonce = parsedPayload["r"]

    let without_proof = "c=biws,r=" & rnonce
    let saltedPass = pbkdf2_hmac_sha256(32, password, salt, iterations.uint32)

    proc stringWithSHA256Digest(d: SHA256Digest): string =
        result = newString(d.len)
        copyMem(addr result[0], unsafeAddr d[0], d.len)

    proc xorBytes(s1, s2: string): string =
        assert(s1.len == s2.len)
        result = newString(s1.len)
        for i in 0 ..< s1.len:
            result[i] = cast[char](cast[uint8](s1[i]) xor cast[uint8](s2[i]))

    let client_key = stringWithSHA256Digest(hmac_sha256(saltedPass, "Client Key"))
    let stored_key = stringWithSHA256Digest(computeSHA256(client_key))

    let auth_msg = join([fb, responsePayload, withoutProof], ",")

    let client_sig = stringWithSHA256Digest(hmac_sha256(stored_key, auth_msg))

    let toEncode = xorBytes(client_key, client_sig)

    let client_proof = "p=" & base64.encode(toEncode)
    let client_final = join([without_proof, client_proof], ",")

    await s.writeJson(%*{
        "authentication": client_final
    })
    let jj = await s.readJson()
    checkSuccess(jj)

proc readResponse(c: Connection) {.async.} =
    var idBuf = await c.sock.recv(8)
    var lenBuf = await c.sock.recv(4)
    var id: int64
    var len: uint32
    copyMem(addr id, addr idBuf[0], sizeof(id))
    copyMem(addr len, addr lenBuf[0], sizeof(len))
    let jStr = await c.sock.recv(len.int)
    var j = parseJson(jStr)
    let f = c.pendingQueries[id]
    c.pendingQueries.del(id)
    if c.pendingQueries.len > 0:
        asyncCheck c.readResponse()

    let t = j["t"].num.ResponseType
    case t
    of SUCCESS_ATOM: j = j["r"][0]
    of SUCCESS_SEQUENCE: j = j["r"]
    else:
        echo "Bad response: ", j
        raise newException(Exception, "Bad response")
    f.complete(j)

proc wrapInStart(q: QueryNode): QueryNode =
    result = newJArray()
    result.add(%Command.start.int)
    result.add(q)
    #result.add(newJObject())

proc runQuery*(c: Connection, q: QueryNode): Future[JsonNode] =
    inc c.queryIdCounter
    var id = c.queryIdCounter
    var serialized = $wrapInStart(q)
    #echo "RUN QUERY: ", serialized
    var len = uint32(serialized.len)
    var message = newString(sizeof(id) + sizeof(len) + int(len))
    copyMem(addr message[0], addr id, sizeof(id))
    copyMem(addr message[8], addr len, sizeof(len))
    copyMem(addr message[12], addr serialized[0], len)
    result = newFuture[JsonNode]()
    c.pendingQueries[id] = result
    asyncCheck c.sock.send(message)
    if c.pendingQueries.len == 1:
        asyncCheck c.readResponse()

proc newConnection*(host: string, username: string = "admin", password: string = ""): Future[Connection] {.async.} =
    let s = newAsyncSocket()
    await s.connect(host, Port(28015))
    var header = 0x34c2bdc3'u32
    await s.send(addr header, sizeof(header))
    let j = await s.readJson()
    await s.authenticate(username, password)
    result.new()
    result.sock = s
    result.pendingQueries = initTable[int64, Future[JsonNode]]()

################################################################################
# Commands

proc cmd(c: Command, args: varargs[JsonNode]): JsonNode = %[%c.int, %args]

template wrapArray(content: JsonNode): JsonNode = %[%Command.makeArray.int, content]


template wrapFunc(body: ExpressionNode): FunctionNode =
    cmd(Command.FUNC, wrapArray(%[58]), body) # What does 58 mean???


template db*(name: string): DBNode = cmd(Command.db, %name)
template table*(theDB: DBNode, name: string): TableNode = cmd(Command.table, theDB, %name)
template get*(table: TableNode, name: string): JsonNode = cmd(Command.GET, table, %name)

proc pluck*(s: SequenceNode, args: varargs[string]): SequenceNode =
    result = %[%Command.PLUCK.int]
    let jArgs = %[s]
    for a in args: jArgs.add(%a)
    result.add(jArgs)

template filter*(sequence: TableNode | SequenceNode, predicate: ExpressionNode): SequenceNode =
    cmd(Command.filter, sequence, wrapFunc(predicate))

template delete*(sequence: SequenceNode): QueryNode =
    cmd(Command.DELETE, sequence)

proc insert*(tab: TableNode, data: JsonNode): QueryNode =
    var data = data
    if data.kind == JArray: data = wrapArray(data)
    cmd(Command.insert, tab, data)

template tableCreate*(theDB: DBNode, name: string): QueryNode = cmd(Command.tableCreate, theDB, %name)
template tableDrop*(theDB: DBNode, name: string): QueryNode = cmd(Command.tableDrop, theDB, %name)
template tableList*(theDB: DBNode): QueryNode = cmd(Command.tableList, theDB)

template row*(name: string): ExpressionNode =
    cmd(Command.BRACKET, cmd(Command.IMPLICIT_VAR), %name)

proc exprIsOp(e: ExpressionNode, c: Command): bool =
    e.kind == JArray and e.len > 0 and e[0].num == c.int

template chainOp(c: Command, a, b: ExpressionNode): ExpressionNode =
    # result = newJArray()
    # result.add(%c.int)
    # let args = newJArray()
    # if exprIsOp(a, c):
    #     if exprIsOp(b, c):
    #         for a in a[0]
    cmd(c, a, b)

template `or`*(a, b: ExpressionNode): ExpressionNode =
    chainOp(Command.OR, a, b)

template `and`*(a, b: ExpressionNode): ExpressionNode =
    chainOp(Command.AND, a, b)

template newExpr*(s: string | int | float): ExpressionNode = %s

template `==`*(n: ExpressionNode, s: string): ExpressionNode =
    cmd(Command.EQ, n, %s)

template `>`*(a: ExpressionNode, b: int): ExpressionNode = cmd(Command.GT, a, %b)
template `<`*(a: ExpressionNode, b: int): ExpressionNode = cmd(Command.LT, a, %b)


template `+`*(a: ExpressionNode, b: ExpressionNode): ExpressionNode = cmd(Command.ADD, a, %b)


    # [
    #                     170, // BRACKET
    #                     [
    #                        [
    #                           13, // IMPLICIT_VAR
    #                           [

    #                           ]
    #                        ],
    #                        "1"
    #                     ]
    #                  ]