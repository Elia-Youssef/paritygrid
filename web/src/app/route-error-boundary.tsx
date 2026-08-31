import { Component, createRef, type ReactNode } from "react";
import { Link } from "react-router";

import { ErrorState } from "../components/states/error-state";
import { Button } from "../components/ui/button";

interface RouteErrorBoundaryProps {
  children: ReactNode;
}

interface RouteErrorBoundaryState {
  error: Error | null;
}

/**
 * Wraps route content inside the shell. A failing screen surfaces a bounded,
 * generic description and records only the boundary category; neither the
 * fallback nor its diagnostic log carries error details that could contain
 * protected content. The shell chrome, navigation, and recovery actions
 * remain available.
 */
export class RouteErrorBoundary extends Component<
  RouteErrorBoundaryProps,
  RouteErrorBoundaryState
> {
  state: RouteErrorBoundaryState = { error: null };

  private readonly fallbackRef = createRef<HTMLDivElement>();

  static getDerivedStateFromError(error: Error): RouteErrorBoundaryState {
    return { error };
  }

  componentDidCatch(): void {
    console.error("ParityGrid render failure", { boundary: "route" });
  }

  componentDidMount(): void {
    if (this.state.error !== null) {
      this.fallbackRef.current?.focus();
    }
  }

  componentDidUpdate(
    _previousProps: RouteErrorBoundaryProps,
    previousState: RouteErrorBoundaryState,
  ): void {
    if (previousState.error === null && this.state.error !== null) {
      this.fallbackRef.current?.focus();
    }
  }

  private readonly reset = (): void => {
    this.setState({ error: null });
  };

  render(): ReactNode {
    const { error } = this.state;
    if (error === null) {
      return this.props.children;
    }

    return (
      <ErrorState
        title="This screen could not render"
        description="An unexpected client error occurred. Retry the screen or return to the operations overview."
        focusRef={this.fallbackRef}
        action={
          <>
            <Button type="button" onClick={this.reset}>
              Try again
            </Button>
            <Button asChild variant="secondary">
              <Link to="/app">Operations overview</Link>
            </Button>
          </>
        }
      />
    );
  }
}
