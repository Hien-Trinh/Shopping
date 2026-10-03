export const meta = {
  name: 'lean-review',
  description: 'Lean PR review: Sonnet finders scaled to diff size, dedupe, at most 5 Sonnet verifiers',
  whenToUse: 'Steps 3-4 of the /lean-review skill. args: {pr, repo, diff, scratch, intent, lines, docsOnly, storage, focus?: {angle: hint}}',
  phases: [
    { title: 'Find', detail: '1-11 reviewer-* finders, by diff size' },
    { title: 'Dedupe', detail: 'merge candidates naming the same mechanism' },
    { title: 'Verify', detail: 'one Sonnet verifier per unreproduced medium/high candidate, max 5' },
  ],
}

const { pr, repo, diff, scratch, intent, lines, docsOnly, storage, focus = {} } = args
const INTENT = `PR #${pr} (checked out in ${repo}). ${intent}`

// Finders scale with the diff (lines = additions + deletions, tests included).
// The spec lane runs at every size; standards from 51 lines (both after Matt Pocock's two axes).
const ANGLES = docsOnly
  ? ['spec']
  : lines <= 50
    ? ['correctness', 'tests', 'failure', 'spec']
    : lines <= 150
      ? ['correctness', 'tests', 'failure', 'spec', 'standards', storage ? 'concurrency' : 'edge']
      : ['correctness', 'concurrency', 'perf', 'tests', 'spec', 'standards', 'failure', 'security', 'data', 'observability', 'edge']
log(`${lines} changed lines${docsOnly ? ' (docs/tooling only)' : ''}: ${ANGLES.length} finder(s): ${ANGLES.join(', ')}`)

const FINDINGS = {
  type: 'object',
  properties: {
    candidates: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          file: { type: 'string' }, line: { type: 'integer' }, defect: { type: 'string' },
          scenario: { type: 'string' }, reproduced: { type: 'boolean' },
          severity: { type: 'string', enum: ['high', 'medium', 'low'] },
        },
        required: ['file', 'line', 'defect', 'scenario', 'reproduced', 'severity'],
      },
    },
    // Not defects: scope decisions for the user (spec), smells (standards). Never ranked with bugs.
    asides: {
      type: 'array',
      items: {
        type: 'object',
        properties: { kind: { type: 'string', enum: ['decision', 'judgement'] }, text: { type: 'string' } },
        required: ['kind', 'text'],
      },
    },
  },
  required: ['candidates'],
}

phase('Find')
const found = await parallel(ANGLES.map(angle => () =>
  agent(
    `${INTENT}\n\nDiff: ${diff}\nYour scratch dir (create it; put every script there, never modify the repo): ${scratch}/${angle}/\n${focus[angle] ? `\nLane hints (${angle}): ${focus[angle]}\n` : ''}\nReturn up to 4 real candidates in your lane; an empty list is a fine answer. Prove with a scratch script when you can and set reproduced accordingly.`,
    { label: `find:${angle}`, phase: 'Find', agentType: `reviewer-${angle}`, schema: FINDINGS },
  ).then(r => (r ? { candidates: r.candidates.map(c => ({ ...c, source: angle })), asides: (r.asides || []).map(a => ({ ...a, source: angle })) } : { candidates: [], asides: [] }))
))
const ok = found.filter(Boolean)  // a finder that crashed resolves to null
const all = ok.flatMap(f => f.candidates)
const asides = ok.flatMap(f => f.asides)
log(`${all.length} raw candidates, ${asides.length} asides (decisions and judgement calls)`)
if (!all.length) return { finders: ANGLES, raw: 0, results: [], asides }

phase('Dedupe')
const DEDUPE = {
  type: 'object',
  properties: {
    unique: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          id: { type: 'string' }, file: { type: 'string' }, line: { type: 'integer' },
          defect: { type: 'string' }, scenario: { type: 'string' },
          sources: { type: 'array', items: { type: 'string' } },
          reproduced: { type: 'boolean' }, severity: { type: 'string', enum: ['high', 'medium', 'low'] },
        },
        required: ['id', 'file', 'line', 'defect', 'scenario', 'sources', 'reproduced', 'severity'],
      },
    },
  },
  required: ['unique'],
}
const asIs = all.map((c, i) => ({ ...c, id: `c${i}`, sources: [c.source] }))
const deduped = all.length < 3 ? null : await agent(
  `Merge these code-review candidates for PR #${pr} so each distinct mechanism appears once. Same root cause = same candidate, even with different wording or lines. Keep the clearest defect and the most concrete scenario, union the sources, reproduced = true if any source reproduced it, severity = the highest. Give each a short kebab-case id. Do not drop or judge anything; only merge.\n\n${JSON.stringify(all, null, 1)}`,
  { label: 'dedupe', phase: 'Dedupe', schema: DEDUPE, model: 'sonnet', effort: 'low' },
)
const unique = deduped ? deduped.unique : asIs

phase('Verify')
const VERDICT = {
  type: 'object',
  properties: { real: { type: 'boolean' }, reason: { type: 'string' }, evidence: { type: 'string' } },
  required: ['real', 'reason', 'evidence'],
}
const MAX_VERIFIERS = 5
const needs = unique.filter(c => c.severity !== 'low' && !c.reproduced)
const sent = needs.slice(0, MAX_VERIFIERS)
const inContext = unique.filter(c => !sent.includes(c))
if (needs.length > sent.length) log(`${needs.length - sent.length} candidate(s) over the verifier cap: verify them in-context`)
log(`${sent.length} verifier(s); ${inContext.length} candidate(s) left for in-context verification`)
const verified = await parallel(sent.map(c => () =>
  agent(
    `${INTENT}\n\nCandidate "${c.id}" (from finders: ${c.sources.join(', ')}):\n- where: ${c.file}:${c.line}\n- defect: ${c.defect}\n- scenario: ${c.scenario}\n\nDecide whether it is real. If a small script can trigger it against the real code on this branch (run from the repo root with \`uv run\`, PYTHONPATH=tests for test helpers), reproduce it and paste the observed output. Otherwise try hard to refute it by reading the code paths end to end and quote the lines. Keep it cheap: about 8 tool calls. Scratch dir (create it; never modify the repo): ${scratch}/verify/${c.id}/`,
    { label: `verify:${c.id}`, phase: 'Verify', schema: VERDICT, model: 'sonnet' },
  ).then(v => ({ ...c, verdict: v }))
))
return { finders: ANGLES, raw: all.length, results: [...verified.filter(Boolean), ...inContext], asides }
