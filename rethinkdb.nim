import asyncdispatch, asyncnet, json, tables
import scram/client

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
    ExpressionNode* = distinct JsonNode

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

        PLUCK = 33
        WITHOUT = 34

        filter = 39

        UPDATE = 53
        DELETE = 54
        INSERT = 56

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
    while true:
        var c: char
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
    if not j{"success"}.getBVal():
        raise newException(Exception, "Authentication error")

proc authenticate(s: AsyncSocket, username, password: string) {.async.} =
    let scramClient = newScramClient[SHA256Digest]()
    let clientFirstMessage = scramClient.prepareFirstMessage(username)

    await s.writeJson(%*{
        "protocol_version": 0,
        "authentication_method": "SCRAM-SHA-256",
        "authentication": clientFirstMessage
    })

    let j = await s.readJson()
    checkSuccess(j)

    let clientFinalMessage = scramClient.prepareFinalMessage(password, j["authentication"].str)
    await s.writeJson(%*{
        "authentication": clientFinalMessage
    })
    checkSuccess(await s.readJson())

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

proc wrapInStart(q: QueryNode): QueryNode = %[%Command.start.int, q]

proc runQueryImpl(c: Connection, q: JsonNode): Future[JsonNode] =
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

template runQuery*(c: Connection, q: QueryNode | ExpressionNode): Future[JsonNode] =
    runQueryImpl(c, JsonNode(q))

proc newConnection*(host = "localhost", username = "admin", password = "", port = 28015): Future[Connection] {.async.} =
    let s = newAsyncSocket()
    await s.connect(host, Port(port))
    var header = 0x34c2bdc3'u32
    await s.send(addr header, sizeof(header))
    checkSuccess(await s.readJson())
    await s.authenticate(username, password)
    result.new()
    result.sock = s
    result.pendingQueries = initTable[int64, Future[JsonNode]]()

proc close*(c: Connection) {.async.} =
    c.sock.close()

################################################################################
# Commands

template cmd(c: Command, args: varargs[JsonNode]): JsonNode = %[%c.int, %args]
template ecmd(c: Command, args: varargs[JsonNode]): ExpressionNode = cmd(c, args).ExpressionNode

template wrapArray(content: JsonNode): JsonNode = %[%Command.makeArray.int, content]


template wrapFunc(body: ExpressionNode): FunctionNode =
    cmd(Command.FUNC, wrapArray(%[58]), body.JsonNode) # What does 58 mean???


template db*(name: string): DBNode = cmd(Command.db, %name)
template table*(theDB: DBNode, name: string): TableNode = cmd(Command.table, theDB, %name)
template get*(table: TableNode, name: string): JsonNode = cmd(Command.GET, table, %name)

proc pluckOrWithoutCmd(c: Command, s: SequenceNode, args: varargs[string]): SequenceNode =
    result = %[%c.int]
    let jArgs = %[s]
    for a in args: jArgs.add(%a)
    result.add(jArgs)

template pluck*(s: SequenceNode, args: varargs[string]): SequenceNode = pluckOrWithoutCmd(Command.PLUCK, s, args)
template `without`*(s: SequenceNode, args: varargs[string]): SequenceNode = pluckOrWithoutCmd(Command.WITHOUT, s, args)
template excludeFields*(s: SequenceNode, args: varargs[string]): SequenceNode = `without`(s, args)

template filter*(sequence: TableNode | SequenceNode, predicate: ExpressionNode): SequenceNode =
    cmd(Command.filter, sequence, wrapFunc(predicate))

template update*(sequence: SequenceNode, o: JsonNode): QueryNode = cmd(Command.UPDATE, sequence, o)
template delete*(sequence: SequenceNode): QueryNode = cmd(Command.DELETE, sequence)

proc insert*(tab: TableNode, data: JsonNode): QueryNode =
    var data = data
    if data.kind == JArray: data = wrapArray(data)
    cmd(Command.INSERT, tab, data)

template tableCreate*(theDB: DBNode, name: string): QueryNode = cmd(Command.tableCreate, theDB, %name)
template tableDrop*(theDB: DBNode, name: string): QueryNode = cmd(Command.tableDrop, theDB, %name)
template tableList*(theDB: DBNode): QueryNode = cmd(Command.tableList, theDB)

template row*(name: string): ExpressionNode =
    ecmd(Command.BRACKET, cmd(Command.IMPLICIT_VAR), %name)

proc exprIsOp(e: ExpressionNode, c: Command): bool =
    let e = e.JsonNode
    e.kind == JArray and e.len > 0 and e[0].num == c.int

template binOp(c: Command, a, b: ExpressionNode): ExpressionNode = ecmd(c, a.JsonNode, b.JsonNode)

template chainOp(c: Command, a, b: ExpressionNode): ExpressionNode =
    # result = newJArray()
    # result.add(%c.int)
    # let args = newJArray()
    # if exprIsOp(a, c):
    #     if exprIsOp(b, c):
    #         for a in a[0]
    binOp(c, a, b)

template `or`*(a, b: ExpressionNode): ExpressionNode = chainOp(Command.OR, a, b)
template `and`*(a, b: ExpressionNode): ExpressionNode = chainOp(Command.AND, a, b)

template newExpr*(s: string | int | float): ExpressionNode = ExpressionNode(%s)

template `==`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.EQ, a, b)
template `!=`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.NE, a, b)

template `>`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.GT, a, b)
template `<`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.LT, a, b)
template `>=`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.GE, a, b)
template `<=`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.LE, a, b)

template `+`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.ADD, a, b)
template `-`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.SUB, a, b)
template `*`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.MUL, a, b)
template `/`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.DIV, a, b)
template `mod`*(a, b: ExpressionNode): ExpressionNode = binOp(Command.MOD, a, b)

template `not`*(a: ExpressionNode): ExpressionNode = ecmd(Command.NOT, a.JsonNode)

template `>`*(a: ExpressionNode, b: int): ExpressionNode = a > newExpr(b)
template `<`*(a: ExpressionNode, b: int): ExpressionNode = a < newExpr(b)

template `==`*(n: ExpressionNode, s: string): ExpressionNode = n == newExpr(s)
