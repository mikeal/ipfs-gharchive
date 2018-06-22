/* istanbul ignore file */
const countSlash = s => (s.match(/\//g) || []).length

module.exports = event => {
  let repo
  if (event.repo) {
    repo = event.repo.name
  } else if (event.repository) {
    repo = `${event.repository.owner}/${event.repository.name}`
  } else {
    if (event.type === 'CreateEvent') {
      repo = event.url.slice('https://github.com/'.length)
      let _owner = repo.slice(0, repo.indexOf('/'))
      repo = repo.slice(0, repo.indexOf('/', _owner.length + 1))
    }
  }
  if (!repo) throw new Error('No repo info.')
  if (countSlash(repo) !== 1) {
    console.error(event)
    throw new Error(`Invalid repo "${repo}"`)
  }
  if (!repo.split('/')[1].length) {
    console.error(event)
    throw new Error(`Invalid repo "${repo}"`)
  }
  return repo
}
