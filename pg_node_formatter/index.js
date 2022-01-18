const { buildLexer } = require('./lexer')

const INDENT_SIZE = 2

function main(input) {
  const tokens = buildLexer(input)
  const state = { output: '', indent: 0 }

  state.writeNew = (value) => {
    const nl = value.output === '' ? '' : '\n'
    state.output += nl + ' '.repeat(state.indent * INDENT_SIZE) + value
  }

  state.write = (value) => {
    state.output += value
  }

  try {
    visitValue(tokens, state)
    console.log(state.output)
  } catch (err) {
    console.log(state.output)
    console.error(err)
  }
}

function wrongToken(token) {
  throw new Error(`Unexpected token '${token.type}': ${token.value}`)
}

function visitValue(tokens, state) {
  const token = tokens.peek()
  switch (token.type) {
    case 'nodeStart':
      visitNode(tokens, state)
      break
    case 'arrayStart':
      visitArray(tokens, state)
      break
    case 'atom':
    case 'string':
      tokens.next()
      state.write(token.value)
      break
    default:
      wrongToken(token)
      break
  }
}

function visitNode(tokens, state) {
  let token = tokens.next('nodeStart')
  state.write(token.value)
  state.indent++

  token = tokens.next()
  let props = 0
  while (token.type !== 'nodeEnd') {
    if (token.type !== 'key') {
      wrongToken(token)
    }

    state.writeNew(token.value + ' ')
    const nextToken = tokens.peek()
    // An AttrNumber array may have no tokens.
    if (nextToken.type !== 'key' && nextToken.type !== 'nodeEnd') {
      visitValue(tokens, state)
    }

    props++
    token = tokens.next()
  }

  state.indent--
  if (props > 0) {
    state.writeNew('}')
  } else {
    state.write('}')
  }
}

function visitArray(tokens, state) {
  let token = tokens.next('arrayStart')
  state.write(token.value)
  state.indent++

  const isOneLine = token.value !== '('

  let elements = 0

  while (tokens.peek().type !== 'arrayEnd') {
    if (isOneLine) {
      state.write(' ')
    } else {
      state.writeNew('')
    }

    visitValue(tokens, state)
    elements++
  }

  tokens.next('arrayEnd')

  state.indent--
  if (elements > 0 && !isOneLine) {
    state.writeNew(')')
  } else {
    state.write(')')
  }
}

async function readStdin() {
  let result = ''
  for await (const chunk of process.stdin) {
    result += chunk
  }

  return result
}

readStdin().then(main)
