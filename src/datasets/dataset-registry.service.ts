import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { DatasetDescriptor, DatasetProvider, DatasetSource } from './dataset.types';

@Injectable()
export class DatasetRegistryService {
  private readonly logger = new Logger(DatasetRegistryService.name);
  /** Static providers known at boot (one descriptor each). */
  private readonly providers = new Map<string, DatasetProvider>();
  /** Dynamic sources resolved at request time (e.g. user-managed templates). */
  private readonly sources: DatasetSource[] = [];

  register(provider: DatasetProvider): void {
    const key = provider.descriptor.key;
    if (this.providers.has(key)) {
      throw new Error(`DatasetProvider for key "${key}" already registered`);
    }
    this.providers.set(key, provider);
    this.logger.log(`Registered dataset: ${key}`);
  }

  registerSource(source: DatasetSource): void {
    this.sources.push(source);
    this.logger.log(`Registered dynamic dataset source: ${source.constructor.name}`);
  }

  /** Merge static descriptors with everything the dynamic sources expose. */
  async listDescriptors(): Promise<DatasetDescriptor[]> {
    const out: DatasetDescriptor[] = Array.from(this.providers.values()).map((p) => p.descriptor);
    for (const source of this.sources) {
      out.push(...(await source.listDescriptors()));
    }
    return out;
  }

  /** Resolve a provider for `key`, checking static providers then dynamic sources. */
  async resolve(key: string): Promise<DatasetProvider> {
    const provider = this.providers.get(key);
    if (provider) return provider;
    for (const source of this.sources) {
      const resolved = await source.resolve(key);
      if (resolved) return resolved;
    }
    throw new NotFoundException(`No dataset registered for key "${key}"`);
  }
}
