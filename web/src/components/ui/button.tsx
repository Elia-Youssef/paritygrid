import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import type { ButtonHTMLAttributes, Ref } from "react";

import { cn } from "../../lib/cn";

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 rounded-md text-sm font-semibold transition-colors duration-[var(--motion-fast)] ease-[var(--ease-standard)] disabled:pointer-events-none disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--focus-ring)]",
  {
    variants: {
      variant: {
        primary: "bg-primary text-primary-foreground hover:bg-primary/90 shadow-sm",
        secondary:
          "border border-border-strong bg-secondary text-secondary-foreground hover:bg-surface-elevated",
        ghost: "text-muted-strong hover:bg-surface-elevated hover:text-foreground",
        destructive: "bg-failure text-background hover:bg-failure/90",
      },
      size: {
        // Default and icon sizes keep interactive targets at 40px and 36px;
        // compact remains above the 24px WCAG minimum for dense toolbars.
        default: "h-10 px-4",
        compact: "h-8 px-3 text-xs",
        icon: "size-9 p-0",
      },
    },
    defaultVariants: {
      variant: "primary",
      size: "default",
    },
  },
);

export interface ButtonProps
  extends ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof buttonVariants> {
  asChild?: boolean;
  ref?: Ref<HTMLButtonElement>;
}

export function Button({
  asChild = false,
  className,
  size,
  variant,
  ref,
  ...props
}: ButtonProps) {
  const Component = asChild ? Slot : "button";

  return (
    <Component
      ref={ref}
      className={cn(buttonVariants({ size, variant }), className)}
      {...props}
    />
  );
}
