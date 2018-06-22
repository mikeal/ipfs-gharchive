const {test} = require('tap')
const database = require('../src/database')

test('invalid link', t => {
  t.plan(1)
  try {
    database.toLink({})
  } catch (e) {
    t.same(e.message, 'Unknown type, cannot create link.')
  }
})
