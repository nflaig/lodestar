import {parentPort, workerData} from "node:worker_threads";
import type {JournalReply, JournalRequest, JournalWorkerData} from "./asyncCommitmentJournal.js";
import {CommitmentJournal} from "./commitmentJournal.js";

const port = parentPort;
if (!port) throw Error("COMMITMENT_WORKER_PORT");
const {directory, identity} = workerData as JournalWorkerData;
const journal = new CommitmentJournal(directory, identity);
port.on("message", ({id, operation}: JournalRequest) => {
  let error: string | undefined;
  try {
    switch (operation.kind) {
      case "retain":
        journal.retain(operation.bid, operation.stored);
        break;
      case "reveal":
        journal.retainReveal(operation.record);
        break;
      case "prune":
        journal.prune(operation.slot);
        break;
    }
  } catch (cause) {
    error = cause instanceof Error ? cause.message : String(cause);
  }
  port.postMessage({id, error} satisfies JournalReply);
});
port.postMessage({id: 0} satisfies JournalReply);
