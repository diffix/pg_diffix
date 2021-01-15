const moo = require('moo')

function buildLexer(input) {
  const lexer = moo.compile({
    whitespace: { match: /[\s\r\n]+/, lineBreaks: true },
    string: /"(?:\\["\\]|[^\n"\\])*"/,
    key: /:[A-Za-z0-9_]+/,
    nodeStart: /\{[A-Za-z0-9_]+/,
    nodeEnd: '}',
    arrayStart: /\([a-z]?/,
    arrayEnd: ')',
    // Square brackets pattern is a hack for constants.
    atom: /(?:[A-Za-z0-9_<>.?\-]+|\s*\[(?:\s*-?\d(?:.\d*)?)*\s*\])+/
  })

  lexer.reset(input)

  function readNext() {
    let result = lexer.next()
    while (result && result.type === 'whitespace') {
      result = lexer.next()
    }

    return result
  }

  let current = readNext()

  function peek() {
    return current
  }

  function next(expectedType) {
    if (
      typeof expectedType === 'string' &&
      (!current || current.type !== expectedType)
    ) {
      throw new Error(`Expected '${expectedType}'.`)
    }

    const old = current
    current = readNext()
    return old
  }

  return {
    peek,
    next
  }
}

module.exports = { buildLexer }
