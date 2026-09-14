import { signal } from '@angular/core';

/**
 * Plays a showcase as steps made of frames, each frame shown for its own time, and loops over the steps.
 * When it isn't playing, a step shows its last frame; with reduced motion it never plays.
 */
export class StepPlayer {
  readonly step = signal(0);
  readonly frame = signal(0);
  readonly playing = signal(false);
  /** Goes up every time a step starts, so its progress bar can start over. */
  readonly run = signal(0);

  private timer?: ReturnType<typeof setTimeout>;
  private readonly reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  /** @param frames for each step, how long each of its frames shows, in ms */
  constructor(private readonly frames: number[][]) {
    this.frame.set(frames[0].length - 1);
  }

  /** How long a step plays, in ms. */
  duration(step: number): number {
    return this.frames[step].reduce((total, ms) => total + ms, 0);
  }

  /** Jumps to a step: from its first frame while playing, otherwise straight to its last. */
  go(step: number) {
    clearTimeout(this.timer);
    this.step.set(step);
    this.run.update(run => run + 1);
    if (this.playing()) {
      this.frame.set(0);
      this.schedule();
    } else {
      this.frame.set(this.frames[step].length - 1);
    }
  }

  play() {
    if (this.reducedMotion) return;
    this.playing.set(true);
    this.go(this.step());
  }

  pause() {
    clearTimeout(this.timer);
    this.playing.set(false);
  }

  toggle() {
    if (this.playing()) this.pause(); else this.play();
  }

  destroy() {
    clearTimeout(this.timer);
  }

  private schedule() {
    this.timer = setTimeout(() => {
      if (this.frame() < this.frames[this.step()].length - 1) {
        this.frame.update(frame => frame + 1);
        this.schedule();
      } else {
        this.go((this.step() + 1) % this.frames.length);
      }
    }, this.frames[this.step()][this.frame()]);
  }
}
