export class RunSequencer {
  #value = 0;

  next(): number {
    this.#value += 1;
    return this.#value;
  }
}
