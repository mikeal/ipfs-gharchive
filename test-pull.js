const createGraph = require('./src/graph')
const createStore = require('ipld-store')

const p = '/data/gharchive.ipld'
let store = createStore(p, false /* no safety */)

console.log({db: p})

// const cbor = require('ipld-complex-graph-builder/fancy-cbor')(() => {})

/* serializer only works for objects with less than 500 keys */
// let serialize = async obj => {
//   return (await Promise.all(cbor.serialize(obj)))[0]
// }
// let empty = (async () => {
//   let block = await serialize({})
//   return block
// })()

const run = async (cid) => {
  // let block = await empty
  // await store.put(block.cid, block.data)
  let graph = createGraph(store, cid)
  for await (let info of graph.pull(1514764800000, true)) {
    console.log(info)
  }
}
run('zdpuAkrtCB9JXPrrKqjLs71rU2Fm6TZmg5dJV76EEQWkbUSbn')
