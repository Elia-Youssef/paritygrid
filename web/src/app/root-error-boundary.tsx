import { Component, createRef, type ReactNode } from "react";

interface RootErrorBoundaryState {
  error: Error | null;
}

/**
 * Last-resort boundary around the whole application, including the router.
 * It is deliberately free of router or shell dependencies so it can render
 * even when those fail, and it offers a real reload plus plain links.
 */
export class RootErrorBoundary extends Component<
  { children: ReactNode },
  RootErrorBoundaryState
> {
  state: RootErrorBoundaryState = { error: null };

  private readonly fallbackRef = createRef<HTMLDivElement>();

  static getDerivedStateFromError(error: Error): RootErrorBoundaryState {
    return { error };
  }

  componentDidCatch(): void {
    console.error("ParityGrid render failure", { boundary: "root" });
  }

  componentDidMount(): void {
    if (this.state.error !== null) {
      this.fallbackRef.current?.focus();
    }
  }

  componentDidUpdate(
    _previousProps: { children: ReactNode },
    previousState: RootErrorBoundaryState,
  ): void {
    if (previousState.error === null && this.state.error !== null) {
      this.fallbackRef.current?.focus();
    }
  }

  private readonly reload = (): void => {
    window.location.reload();
  };

  render(): ReactNode {
    const { error } = this.state;
    if (error === null) {
      return this.props.children;
    }

    return (
      <div
        ref={this.fallbackRef}
        role="alert"
        tabIndex={-1}
        className="flex min-h-screen flex-col items-start justify-center gap-4 bg-background p-8 text-foreground"
      >
        <h1 className="text-lg font-semibold">ParityGrid could not start</h1>
        <p className="max-w-xl text-sm text-muted-strong">
          An unexpected client error prevented the application from rendering. Reload
          the application to recover.
        </p>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={this.reload}
            className="inline-flex h-10 items-center rounded-md bg-primary px-4 text-sm font-semibold text-primary-foreground"
          >
            Reload
          </button>
          <a
            href="/"
            className="inline-flex h-10 items-center rounded-md border border-border-strong bg-secondary px-4 text-sm font-semibold text-secondary-foreground"
          >
            Public overview
          </a>
        </div>
      </div>
    );
  }
}
