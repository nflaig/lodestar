// Isolated instrumentation of the real journal. Never loaded by production.
import fs from 'node:fs';
import {parentPort, workerData} from 'node:worker_threads';
import {pathToFileURL} from 'node:url';
const {CommitmentJournal} = await import(pathToFileURL(`${workerData.root}/packages/builder/lib/services/commitmentJournal.js`));
let profile;
for (const name of ['writeFileSync', 'fsyncSync', 'renameSync']) {
  const original = fs[name];
  fs[name] = function(...args) {
    const start = performance.now();
    try { return original.apply(this, args); }
    finally { if (profile) profile[name] = (profile[name] ?? 0) + performance.now() - start; }
  };
}
for (const name of ['write', 'writeBytes']) {
  const original = CommitmentJournal.prototype[name];
  CommitmentJournal.prototype[name] = function(...args) {
    const start = performance.now();
    try { return original.apply(this, args); }
    finally { if (profile) profile[name] = (profile[name] ?? 0) + performance.now() - start; }
  };
}
const originalRetain = CommitmentJournal.prototype.retain;
CommitmentJournal.prototype.retain = function(...args) {
  profile = {};
  const start = performance.now();
  try { return originalRetain.apply(this, args); }
  finally { profile.retainMs = performance.now() - start; }
};
const originalPost = parentPort.postMessage.bind(parentPort);
parentPort.postMessage = reply => {
  originalPost({...reply, profile});
  profile = undefined;
};
await import(pathToFileURL(`${workerData.root}/packages/builder/lib/services/commitmentJournalWorker.js`));
