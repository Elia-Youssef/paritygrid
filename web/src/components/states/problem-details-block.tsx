import type { ProblemDetailsView } from "../../lib/problem-details";

export interface ProblemDetailsBlockProps {
  problem: ProblemDetailsView;
}

/**
 * Text-only presentation of a bounded Problem Details view model. Every
 * value renders as an inert text node — never markup — and the model has
 * already been capped and redacted by the parser.
 */
export function ProblemDetailsBlock({ problem }: ProblemDetailsBlockProps) {
  const candidateRows: readonly (readonly [string, string | undefined])[] = [
    ["Status", problem.status === undefined ? undefined : String(problem.status)],
    ["Type", problem.type],
    ["Detail", problem.detail],
    ["Instance", problem.instance],
    ...problem.extensions.map((extension): readonly [string, string] => [
      extension.name,
      extension.value,
    ]),
  ];
  const rows = candidateRows.filter(
    (row): row is readonly [string, string] => row[1] !== undefined,
  );

  return (
    <dl className="mt-3 space-y-2 rounded-md border border-border bg-surface-quiet p-4 text-sm">
      <div className="flex flex-wrap gap-x-3">
        <dt className="font-mono text-2xs tracking-label text-muted uppercase">
          Title
        </dt>
        <dd className="font-medium text-foreground">{problem.title}</dd>
      </div>
      {rows.map(([name, value]) => (
        <div key={name} className="flex flex-wrap gap-x-3">
          <dt className="font-mono text-2xs tracking-label text-muted uppercase">
            {name}
          </dt>
          <dd className="min-w-0 break-all font-mono text-xs text-muted-strong">
            {value}
          </dd>
        </div>
      ))}
    </dl>
  );
}
