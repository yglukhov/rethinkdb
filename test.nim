import asyncdispatch, json
import rethinkdb

let c = waitFor newConnection("localhost")

proc setUp() {.async.} =
    var j = await c.runQuery(db("test").tableCreate("hello"))
    echo "create: ", j
    j = await c.runQuery(db("test").tableDrop("hello"))
    echo "drop: ", j
    j = await c.runQuery(db("test").tableCreate("hello"))
    echo "create again: ", j

    j = await c.runQuery(db("test").tableList())
    echo "tableList: ", j

    let t = db("test").table("hello")
    j = await c.runQuery(t)
    echo "table: ", j
    j = await c.runQuery(t.insert(%*[{"qwer": "adf"}, {"qwer1": "asdf1"}]))
    echo "insert: ", j
    j = await c.runQuery(t.insert(%*[{"a": 5}, {"a": 10}, {"a": 15}]))
    echo "insert: ", j
    j = await c.runQuery(t)
    echo "table: ", j

    j = await c.runQuery(t.filter(row("qwer") == "adf"))
    echo "filter: ", j

    j = await c.runQuery(t.filter(row("qwer") == "adf").delete())
    echo "delete: ", j
    j = await c.runQuery(t.filter(row("qwer1") == "asdf1").delete())
    echo "delete: ", j


proc tearDown() {.async.} =
    var j = await c.runQuery(db("test").tableDrop("hello"))
    echo "drop: ", j
    await c.close()

proc concurentTest1(seed: int) {.async.} =
    for i in 0 .. 50:
        let a = i + seed
        let b = 100 * i * seed

        let r = a + b
        let rr = (await c.runQuery newExpr(a) + newExpr(b)).num
        doAssert(rr == r)

proc concurentTest2() {.async.} =
    for i in 0 .. 50:
        let j = await c.runQuery db("test").table("hello").filter(row("a") > 5 and row("a") < 16).pluck("id")
        doAssert(j.len == 2)

proc concurentTest3() {.async.} =
    for i in 0 .. 50:
        var j = await c.runQuery db("test").table("hello").insert(%*{"concurentTest3": i})
        let keys = j["generated_keys"]
        doAssert(keys.len == 1)
        j = await c.runQuery db("test").table("hello").get(keys[0].str)
        doAssert(j["concurentTest3"].num == i)
        j = await c.runQuery db("test").table("hello").get(keys[0].str).delete()
        doAssert(j["deleted"].num == 1)

proc main() =
    defer: waitFor tearDown()
    waitFor setUp()
    waitFor all(
        concurentTest1(10), concurentTest2(), concurentTest1(30),
        concurentTest2(), concurentTest1(50), concurentTest3(),
        concurentTest1(1000), concurentTest3(), concurentTest2())

main()
