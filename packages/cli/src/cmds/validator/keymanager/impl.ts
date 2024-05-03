import bls from "@chainsafe/bls";
import {Keystore} from "@chainsafe/bls-keystore";
import {fromHexString} from "@chainsafe/ssz";
import {
  Endpoints,
  DeleteRemoteKeyStatus,
  DeletionStatus,
  ImportStatus,
  ResponseStatus,
  KeystoreStr,
  PubkeyHex,
  SlashingProtectionData,
  SignerDefinition,
  RemoteSignerDefinition,
  ImportRemoteKeyStatus,
  FeeRecipientData,
  GraffitiData,
  GasLimitData,
  BuilderBoostFactorData,
} from "@lodestar/api/keymanager";
import {Interchange, SignerType, Validator} from "@lodestar/validator";
import {ApiError, ApplicationMethods} from "@lodestar/api/server";
import {Epoch} from "@lodestar/types";
import {isValidHttpUrl} from "@lodestar/utils";
import {getPubkeyHexFromKeystore, isValidatePubkeyHex} from "../../../util/format.js";
import {parseFeeRecipient} from "../../../util/index.js";
import {DecryptKeystoresThreadPool} from "./decryptKeystores/index.js";
import {IPersistedKeysBackend} from "./interface.js";

type Api = ApplicationMethods<Endpoints>;

export class KeymanagerApi implements Api {
  constructor(
    private readonly validator: Validator,
    private readonly persistedKeysBackend: IPersistedKeysBackend,
    private readonly signal: AbortSignal,
    private readonly proposerConfigWriteDisabled?: boolean
  ) {}

  private checkIfProposerWriteEnabled(): void {
    if (this.proposerConfigWriteDisabled === true) {
      throw Error("proposerSettingsFile option activated");
    }
  }

  async listFeeRecipient({pubkey}: {pubkey: PubkeyHex}): ReturnType<Api["listFeeRecipient"]> {
    return {data: {pubkey, ethaddress: this.validator.validatorStore.getFeeRecipient(pubkey)}};
  }

  async setFeeRecipient({pubkey, ethaddress}: FeeRecipientData): ReturnType<Api["setFeeRecipient"]> {
    this.checkIfProposerWriteEnabled();
    this.validator.validatorStore.setFeeRecipient(pubkey, parseFeeRecipient(ethaddress));
    this.persistedKeysBackend.writeProposerConfig(pubkey, this.validator.validatorStore.getProposerConfig(pubkey));
    return {status: 202};
  }

  async deleteFeeRecipient({pubkey}: {pubkey: PubkeyHex}): ReturnType<Api["deleteFeeRecipient"]> {
    this.checkIfProposerWriteEnabled();
    this.validator.validatorStore.deleteFeeRecipient(pubkey);
    this.persistedKeysBackend.writeProposerConfig(pubkey, this.validator.validatorStore.getProposerConfig(pubkey));
    return {status: 204};
  }

  async listGraffiti({pubkey}: {pubkey: PubkeyHex}): ReturnType<Api["listGraffiti"]> {
    return {data: {pubkey, graffiti: this.validator.validatorStore.getGraffiti(pubkey)}};
  }

  async setGraffiti({pubkey, graffiti}: GraffitiData): ReturnType<Api["setGraffiti"]> {
    this.checkIfProposerWriteEnabled();
    this.validator.validatorStore.setGraffiti(pubkey, graffiti);
    this.persistedKeysBackend.writeProposerConfig(pubkey, this.validator.validatorStore.getProposerConfig(pubkey));
    return {status: 202};
  }

  async deleteGraffiti({pubkey}: {pubkey: PubkeyHex}): ReturnType<Api["deleteGraffiti"]> {
    this.checkIfProposerWriteEnabled();
    this.validator.validatorStore.deleteGraffiti(pubkey);
    this.persistedKeysBackend.writeProposerConfig(pubkey, this.validator.validatorStore.getProposerConfig(pubkey));
    return {status: 204};
  }

  async getGasLimit({pubkey}: {pubkey: PubkeyHex}): ReturnType<Api["getGasLimit"]> {
    const gasLimit = this.validator.validatorStore.getGasLimit(pubkey);
    return {data: {pubkey, gasLimit}};
  }

  async setGasLimit({pubkey, gasLimit}: GasLimitData): ReturnType<Api["setGasLimit"]> {
    this.checkIfProposerWriteEnabled();
    this.validator.validatorStore.setGasLimit(pubkey, gasLimit);
    this.persistedKeysBackend.writeProposerConfig(pubkey, this.validator.validatorStore.getProposerConfig(pubkey));
    return {status: 202};
  }

  async deleteGasLimit({pubkey}: {pubkey: PubkeyHex}): ReturnType<Api["deleteGasLimit"]> {
    this.checkIfProposerWriteEnabled();
    this.validator.validatorStore.deleteGasLimit(pubkey);
    this.persistedKeysBackend.writeProposerConfig(pubkey, this.validator.validatorStore.getProposerConfig(pubkey));
    return {status: 204};
  }

  /**
   * List all validating pubkeys known to and decrypted by this keymanager binary
   *
   * https://github.com/ethereum/keymanager-APIs/blob/0c975dae2ac6053c8245ebdb6a9f27c2f114f407/keymanager-oapi.yaml
   */
  async listKeys(): ReturnType<Api["listKeys"]> {
    const pubkeys = this.validator.validatorStore.votingPubkeys();
    return {
      data: pubkeys.map((pubkey) => ({
        validatingPubkey: pubkey,
        derivationPath: "",
        readonly: this.validator.validatorStore.getSigner(pubkey)?.type !== SignerType.Local,
      })),
    };
  }

  /**
   * Import keystores generated by the Eth2.0 deposit CLI tooling. `passwords[i]` must unlock `keystores[i]`.
   *
   * Users SHOULD send slashing_protection data associated with the imported pubkeys. MUST follow the format defined in
   * EIP-3076: Slashing Protection Interchange Format.
   *
   * @param keystores JSON-encoded keystore files generated with the Launchpad
   * @param passwords Passwords to unlock imported keystore files. `passwords[i]` must unlock `keystores[i]`
   * @param slashingProtection Slashing protection data for some of the keys of `keystores`
   * @returns Status result of each `request.keystores` with same length and order of `request.keystores`
   *
   * https://github.com/ethereum/keymanager-APIs/blob/0c975dae2ac6053c8245ebdb6a9f27c2f114f407/keymanager-oapi.yaml
   */
  async importKeystores({
    keystores,
    passwords,
    slashingProtection,
  }: {
    keystores: KeystoreStr[];
    passwords: string[];
    slashingProtection?: SlashingProtectionData;
  }): ReturnType<Api["importKeystores"]> {
    if (slashingProtection) {
      // The arguments to this function is passed in within the body of an HTTP request
      // hence fastify will parse it into an object before this function is called.
      // Even though the slashingProtection is typed as SlashingProtectionData,
      // at runtime, when the handler for the request is selected, it would see slashingProtection
      // as an object, hence trying to parse it using JSON.parse won't work. Instead, we cast straight to Interchange
      const interchange = ensureJSON<Interchange>(slashingProtection);
      await this.validator.importInterchange(interchange);
    }

    const statuses: {status: ImportStatus; message?: string}[] = [];
    const decryptKeystores = new DecryptKeystoresThreadPool(keystores.length, this.signal);

    for (let i = 0; i < keystores.length; i++) {
      try {
        const keystoreStr = keystores[i];
        const password = passwords[i];
        if (password === undefined) {
          throw new ApiError(400, `No password for keystores[${i}]`);
        }

        const keystore = Keystore.parse(keystoreStr);
        const pubkeyHex = getPubkeyHexFromKeystore(keystore);

        // Check for duplicates and skip keystore before decrypting
        if (this.validator.validatorStore.hasVotingPubkey(pubkeyHex)) {
          statuses[i] = {status: ImportStatus.duplicate};
          continue;
        }

        decryptKeystores.queue(
          {keystoreStr, password},
          async (secretKeyBytes: Uint8Array) => {
            const secretKey = bls.SecretKey.fromBytes(secretKeyBytes);

            // Persist the key to disk for restarts, before adding to in-memory store
            // If the keystore exist and has a lock it will throw
            this.persistedKeysBackend.writeKeystore({
              keystoreStr,
              password,
              // Lock immediately since it's gonna be used
              lockBeforeWrite: true,
              // Always write, even if it's already persisted for consistency.
              // The in-memory validatorStore is the ground truth to decide duplicates
              persistIfDuplicate: true,
            });

            // Add to in-memory store to start validating immediately
            await this.validator.validatorStore.addSigner({type: SignerType.Local, secretKey});

            statuses[i] = {status: ImportStatus.imported};
          },
          (e: Error) => {
            statuses[i] = {status: ImportStatus.error, message: e.message};
          }
        );
      } catch (e) {
        statuses[i] = {status: ImportStatus.error, message: (e as Error).message};
      }
    }

    await decryptKeystores.completed();

    return {data: statuses};
  }

  /**
   * DELETE must delete all keys from `request.pubkeys` that are known to the keymanager and exist in its
   * persistent storage. Additionally, DELETE must fetch the slashing protection data for the requested keys from
   * persistent storage, which must be retained (and not deleted) after the response has been sent. Therefore in the
   * case of two identical delete requests being made, both will have access to slashing protection data.
   *
   * In a single atomic sequential operation the keymanager must:
   * 1. Guarantee that key(s) can not produce any more signature; only then
   * 2. Delete key(s) and serialize its associated slashing protection data
   *
   * DELETE should never return a 404 response, even if all pubkeys from request.pubkeys have no extant keystores
   * nor slashing protection data.
   *
   * Slashing protection data must only be returned for keys from `request.pubkeys` for which a
   * `deleted` or `not_active` status is returned.
   *
   * @param pubkeys List of public keys to delete.
   * @returns Deletion status of all keys in `request.pubkeys` in the same order.
   *
   * https://github.com/ethereum/keymanager-APIs/blob/0c975dae2ac6053c8245ebdb6a9f27c2f114f407/keymanager-oapi.yaml
   */
  async deleteKeys({pubkeys}: {pubkeys: PubkeyHex[]}): ReturnType<Api["deleteKeys"]> {
    const deletedKey: boolean[] = [];
    const statuses = new Array<{status: DeletionStatus; message?: string}>(pubkeys.length);

    for (let i = 0; i < pubkeys.length; i++) {
      try {
        const pubkeyHex = pubkeys[i];

        if (!isValidatePubkeyHex(pubkeyHex)) {
          throw new ApiError(400, `Invalid pubkey ${pubkeyHex}`);
        }

        // Skip unknown keys or remote signers
        const signer = this.validator.validatorStore.getSigner(pubkeyHex);
        if (signer && signer.type === SignerType.Local) {
          // Remove key from live local signer
          deletedKey[i] = this.validator.validatorStore.removeSigner(pubkeyHex);

          // Remove key from block duties
          // Remove from attestation duties
          // Remove from Sync committee duties
          // Remove from indices
          this.validator.removeDutiesForKey(pubkeyHex);
        }

        // Attempts to delete everything first, and returns status.
        // This unlocks the keystore, so perform after deleting from in-memory store
        const diskDeleteStatus = this.persistedKeysBackend.deleteKeystore(pubkeyHex);

        if (diskDeleteStatus) {
          // TODO: What if the diskDeleteStatus status is incosistent?
          deletedKey[i] = true;
        }
      } catch (e) {
        statuses[i] = {status: DeletionStatus.error, message: (e as Error).message};
      }
    }

    const pubkeysBytes = pubkeys.map((pubkeyHex) => fromHexString(pubkeyHex));

    const interchangeV5 = await this.validator.exportInterchange(pubkeysBytes, {
      version: "5",
    });

    // After exporting slashing protection data in bulk, render the status
    const pubkeysWithSlashingProtectionData = new Set(interchangeV5.data.map((data) => data.pubkey));
    for (let i = 0; i < pubkeys.length; i++) {
      if (statuses[i]?.status === DeletionStatus.error) {
        continue;
      }
      const status = deletedKey[i]
        ? DeletionStatus.deleted
        : pubkeysWithSlashingProtectionData.has(pubkeys[i])
          ? DeletionStatus.not_active
          : DeletionStatus.not_found;
      statuses[i] = {status};
    }

    return {
      data: {
        statuses,
        slashingProtection: JSON.stringify(interchangeV5),
      },
    };
  }

  /**
   * List all remote validating pubkeys known to this validator client binary
   */
  async listRemoteKeys(): ReturnType<Api["listRemoteKeys"]> {
    const remoteKeys: SignerDefinition[] = [];

    for (const pubkeyHex of this.validator.validatorStore.votingPubkeys()) {
      const signer = this.validator.validatorStore.getSigner(pubkeyHex);
      if (signer && signer.type === SignerType.Remote) {
        remoteKeys.push({pubkey: signer.pubkey, url: signer.url, readonly: false});
      }
    }

    return {
      data: remoteKeys,
    };
  }

  /**
   * Import remote keys for the validator client to request duties for
   */
  async importRemoteKeys({
    remoteSigners,
  }: {
    remoteSigners: RemoteSignerDefinition[];
  }): ReturnType<Api["importRemoteKeys"]> {
    const importPromises = remoteSigners.map(async ({pubkey, url}): Promise<ResponseStatus<ImportRemoteKeyStatus>> => {
      try {
        if (!isValidatePubkeyHex(pubkey)) {
          throw new ApiError(400, `Invalid pubkey ${pubkey}`);
        }
        if (!isValidHttpUrl(url)) {
          throw new ApiError(400, `Invalid URL ${url}`);
        }

        // Check if key exists
        if (this.validator.validatorStore.hasVotingPubkey(pubkey)) {
          return {status: ImportRemoteKeyStatus.duplicate};
        }

        // Else try to add it

        await this.validator.validatorStore.addSigner({type: SignerType.Remote, pubkey, url});

        this.persistedKeysBackend.writeRemoteKey({
          pubkey,
          url,
          // Always write, even if it's already persisted for consistency.
          // The in-memory validatorStore is the ground truth to decide duplicates
          persistIfDuplicate: true,
        });

        return {status: ImportRemoteKeyStatus.imported};
      } catch (e) {
        return {status: ImportRemoteKeyStatus.error, message: (e as Error).message};
      }
    });

    return {
      data: await Promise.all(importPromises),
    };
  }

  /**
   * DELETE must delete all keys from `request.pubkeys` that are known to the validator client and exist in its
   * persistent storage.
   * DELETE should never return a 404 response, even if all pubkeys from request.pubkeys have no existing keystores.
   */
  async deleteRemoteKeys({pubkeys}: {pubkeys: PubkeyHex[]}): ReturnType<Api["deleteRemoteKeys"]> {
    const results = pubkeys.map((pubkeyHex): ResponseStatus<DeleteRemoteKeyStatus> => {
      try {
        if (!isValidatePubkeyHex(pubkeyHex)) {
          throw new ApiError(400, `Invalid pubkey ${pubkeyHex}`);
        }

        const signer = this.validator.validatorStore.getSigner(pubkeyHex);

        // Remove key from live local signer
        const deletedFromMemory =
          signer && signer.type === SignerType.Remote ? this.validator.validatorStore.removeSigner(pubkeyHex) : false;

        if (deletedFromMemory) {
          // Remove duties if key was deleted from in-memory store
          this.validator.removeDutiesForKey(pubkeyHex);
        }

        const deletedFromDisk = this.persistedKeysBackend.deleteRemoteKey(pubkeyHex);

        return {
          status:
            deletedFromMemory || deletedFromDisk ? DeleteRemoteKeyStatus.deleted : DeleteRemoteKeyStatus.not_found,
        };
      } catch (e) {
        return {status: DeleteRemoteKeyStatus.error, message: (e as Error).message};
      }
    });

    return {
      data: results,
    };
  }

  async getBuilderBoostFactor({pubkey}: {pubkey: PubkeyHex}): ReturnType<Api["getBuilderBoostFactor"]> {
    const builderBoostFactor = this.validator.validatorStore.getBuilderBoostFactor(pubkey);
    return {data: {pubkey, builderBoostFactor}};
  }

  async setBuilderBoostFactor({
    pubkey,
    builderBoostFactor,
  }: BuilderBoostFactorData): ReturnType<Api["setBuilderBoostFactor"]> {
    this.checkIfProposerWriteEnabled();
    this.validator.validatorStore.setBuilderBoostFactor(pubkey, builderBoostFactor);
    this.persistedKeysBackend.writeProposerConfig(pubkey, this.validator.validatorStore.getProposerConfig(pubkey));
    return {status: 202};
  }

  async deleteBuilderBoostFactor({pubkey}: {pubkey: PubkeyHex}): ReturnType<Api["deleteBuilderBoostFactor"]> {
    this.checkIfProposerWriteEnabled();
    this.validator.validatorStore.deleteBuilderBoostFactor(pubkey);
    this.persistedKeysBackend.writeProposerConfig(pubkey, this.validator.validatorStore.getProposerConfig(pubkey));
    return {status: 204};
  }

  /**
   * Create and sign a voluntary exit message for an active validator
   */
  async signVoluntaryExit({pubkey, epoch}: {pubkey: PubkeyHex; epoch?: Epoch}): ReturnType<Api["signVoluntaryExit"]> {
    if (!isValidatePubkeyHex(pubkey)) {
      throw new ApiError(400, `Invalid pubkey ${pubkey}`);
    }
    return {data: await this.validator.signVoluntaryExit(pubkey, epoch)};
  }
}

/**
 * Given a variable with JSON that maybe stringified or not, return parsed JSON
 */
function ensureJSON<T>(strOrJson: string | T): T {
  if (typeof strOrJson === "string") {
    return JSON.parse(strOrJson) as T;
  } else {
    return strOrJson;
  }
}
