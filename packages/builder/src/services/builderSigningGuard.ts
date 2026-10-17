import fs from "node:fs";
import path from "node:path";
import {ClassicLevel} from "classic-level";
import {ensureCommitmentDirectory} from "./commitmentJournal.js";

/** The native database lock has no stale timeout: a suspended owner keeps its lock.
 * All replicas must use this same durable directory; copied disks or keys are not fenced. */
export class BuilderSigningGuard {
  private active = true;
  private closing?: Promise<void>;

  private constructor(private readonly ownership: ClassicLevel) {}

  static async acquire(directory: string): Promise<BuilderSigningGuard> {
    ensureCommitmentDirectory(directory);
    const ownerDirectory = path.join(fs.realpathSync(directory), ".writer-lock");
    fs.mkdirSync(ownerDirectory, {mode: 0o700, recursive: true});
    // Unlike the shared beacon database, ownership must not share a native handle
    // with a second builder instance or worker thread in this same process.
    const ownership = new ClassicLevel(ownerDirectory, {multithreading: false});
    try {
      await ownership.open();
      return new BuilderSigningGuard(ownership);
    } catch (error) {
      await ownership.close();
      throw Error("BUILDER_SIGNER_OWNERSHIP_UNAVAILABLE", {cause: error});
    }
  }

  assertActive(): void {
    if (!this.active) throw Error("BUILDER_SIGNER_OWNERSHIP_RELEASED");
  }

  revoke(): void {
    this.active = false;
  }

  close(): Promise<void> {
    this.revoke();
    this.closing ??= this.ownership.close();
    return this.closing;
  }
}
