import asyncdispatch, json
import rethinkdb

let c = waitFor newConnection("localhost")

proc test() {.async.} =
    echo "create: ", await c.runQuery(db("test").tableCreate("yoyoyo"))
    echo "drop: ", await c.runQuery(db("test").tableDrop("yoyoyo"))

    echo "tableList: ", await c.runQuery(db("test").tableList())

    let t = db("test").table("hello")
    echo "table: ", await c.runQuery(t)
    echo "insert: ", await c.runQuery(t.insert(%*[{"qwer": "adf"}, {"qwer1": "asdf1"}]))
    echo "table: ", await c.runQuery(t)

    echo "filter: ", await c.runQuery(t.filter(row("qwer") == "adf"))

    echo "delete: ", await c.runQuery(t.filter(row("qwer") == "adf").delete())
    echo "delete: ", await c.runQuery(t.filter(row("qwer1") == "asdf1").delete())

    #echo "insert: ", await c.runQuery(t.insert(%*[{"qwer": "adf"}, {"qwer1": "asdf1"}]))
    echo "pluck: ", await c.runQuery t.filter(row("a") > 5 and row("a") < 16).pluck("id")

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
    waitFor test()
    waitFor all(
        concurentTest1(10), concurentTest2(), concurentTest1(30),
        concurentTest2(), concurentTest1(50), concurentTest3(),
        concurentTest1(1000), concurentTest3(), concurentTest2())

main()
