import { EventEmitter } from 'events';

export interface CraftLinkOptions {
  url?: string;
  token?: string;
  reconnect?: boolean;
  reconnectBaseMs?: number;
  reconnectMaxMs?: number;
}

export interface PaletteEntry {
  index: number;
  name: string | null;
  sid: number;
  states: Record<string, string> | null;
}

export interface ChunkMessage {
  type: 'subchunk';
  x: number;
  y: number;
  z: number;
  palette: PaletteEntry[];
  indices: string;
  skyLight: string | null;
  blockLight: string | null;
}

export interface BlockUpdateMessage {
  type: 'blockUpdate';
  x: number;
  y: number;
  z: number;
  name?: string;
  sid?: number;
  stateId?: number;
  states?: Record<string, string>;
}

export type EntityCategory = 'player' | 'mob' | 'object';

export interface EntityVelocity {
  x: number;
  y: number;
  z: number;
}

export interface EntityEquipmentItem {
  name: string;
}

export interface EntityMessage {
  type: 'entity';
  id: number;
  name: string;
  entityType: EntityCategory;
  username?: string;
  itemName?: string;
  x: number;
  y: number;
  z: number;
  pos: EntityVelocity;
  yaw: number;
  pitch: number;
  vx: number;
  vy: number;
  vz: number;
  velocity: EntityVelocity;
  metadata: number[];
  equipment?: Array<EntityEquipmentItem | null>;
  health?: number;
  isUsingItem?: boolean;
}

export interface EntityGoneMessage {
  type: 'entityGone';
  id: number;
}

export interface EntityAnimationMessage {
  type: 'entityAnimation';
  id: number;
  animation: string;
}

export interface SoundMessage {
  type: 'sound';
  name: string;
  x: number;
  y: number;
  z: number;
  volume: number;
  pitch: number;
  category: string;
}

export interface ParticleMessage {
  type: 'particle';
  particle: string;
  x: number;
  y: number;
  z: number;
  dx: number;
  dy: number;
  dz: number;
  count: number;
  speed: number;
  data: {
    stateId?: number;
    blockStateId?: number;
    [key: string]: unknown;
  };
}

export interface BlockActionMessage {
  type: 'blockAction';
  x: number;
  y: number;
  z: number;
  actionId: number;
  actionParam: number;
  blockId: number;
}

export interface BlockEntityEntry {
  x: number;
  y: number;
  z: number;
  id: string;
  data: Record<string, unknown>;
}

export interface BlockEntitiesMessage {
  type: 'blockEntities';
  chunk: [number, number];
  entities: BlockEntityEntry[];
}

export interface TimeMessage {
  type: 'time';
  timeOfDay: number;
  age: number;
}

export interface WeatherMessage {
  type: 'weather';
  isRaining: boolean;
  isThundering: boolean;
  rainLevel: number;
}

export type ScoreboardMessage =
  | {
      type: 'scoreboard';
      action: 'setObjective';
      name: string;
      displayName: string;
      renderType: string;
    }
  | {
      type: 'scoreboard';
      action: 'removeObjective';
      name: string;
    }
  | {
      type: 'scoreboard';
      action: 'setScore';
      objective: string;
      entry: string;
      value: number;
      displayName: string | null;
    }
  | {
      type: 'scoreboard';
      action: 'removeScore';
      objective: string | null;
      entry: string;
    }
  | {
      type: 'scoreboard';
      action: 'setDisplay';
      slot: string;
      objective: string | null;
    };

export type BossbarMessage =
  | {
      type: 'bossbar';
      action: 'add';
      id: string;
      name: string;
      progress: number;
      color: string;
      division: string;
      darkenSky: boolean;
      playBossMusic: boolean;
      createWorldFog: boolean;
    }
  | {
      type: 'bossbar';
      action: 'remove';
      id: string;
    }
  | {
      type: 'bossbar';
      action: 'updateProgress';
      id: string;
      progress: number;
    }
  | {
      type: 'bossbar';
      action: 'updateName';
      id: string;
      name: string;
    }
  | {
      type: 'bossbar';
      action: 'updateStyle';
      id: string;
      color: string;
      division: string;
    }
  | {
      type: 'bossbar';
      action: 'updateProperties';
      id: string;
      darkenSky: boolean;
      playBossMusic: boolean;
      createWorldFog: boolean;
    };

export interface ContainerItem {
  slot: number;
  id: string;
  count: number;
  damage?: number;
  displayName?: string;
  enchantments?: Array<{ id: string; level: number }>;
  nbt: Record<string, unknown>;
}

export interface ContainerOpenMessage {
  type: 'containerOpen';
  windowId: number;
  containerType: string;
  title: string;
  slots: number;
}

export interface ContainerCloseMessage {
  type: 'containerClose';
  windowId: number;
}

export interface ContainerContentMessage {
  type: 'containerContent';
  windowId: number;
  items: Array<ContainerItem | null>;
}

export interface ContainerSlotMessage {
  type: 'containerSlot';
  windowId: number;
  slot: number;
  item: ContainerItem | null;
}

export interface BiomePaletteEntry {
  index: number;
  name: string;
}

export interface BiomesMessage {
  type: 'biomes';
  x: number;
  y: number;
  z: number;
  biomes: BiomePaletteEntry[];
  biomeIndices: string;
}

export interface PositionMessage {
  type: 'position';
  x: number;
  y: number;
  z: number;
  yaw: number;
  pitch: number;
}

export interface AllPositionsPlayer {
  id: string;
  name: string;
  world: string;
  x: number;
  y: number;
  z: number;
  yaw: number;
  pitch: number;
  isAnchor: boolean;
}

export interface AllPositionsMessage {
  type: 'allPositions';
  players: AllPositionsPlayer[];
}

export interface StatusMessage {
  type: 'status';
  health: number;
  food: number;
  experience: number;
  level: number;
}

export interface DisconnectedMessage {
  code: number;
  reason: string;
}

export type RawMessage =
  | {
      type: 'ready';
      chunkCount: number;
      ts: number;
    }
  | {
      type: 'heightmap';
      x: number;
      z: number;
      heights: number[];
    }
  | {
      type: 'inventory';
      selectedSlot: number;
      slots: Array<{
        slot: number;
        name: string;
        displayName: string;
        count: number;
        type: number;
      }>;
    }
  | {
      type: 'equipment';
      mainHand: { name: string } | null;
      offHand: { name: string } | null;
      helmet: { name: string } | null;
      chestplate: { name: string } | null;
      leggings: { name: string } | null;
      boots: { name: string } | null;
    }
  | {
      type: 'skin';
      username: string;
      skinUrl?: string;
    }
  | {
      type: 'entitySound';
      name: string;
      entityId: number;
      volume: number;
      pitch: number;
      category: string;
    }
  | {
      type: string;
      [key: string]: unknown;
    };

export class CraftLink extends EventEmitter {
  constructor(options?: CraftLinkOptions);
  connect(): Promise<void>;
  disconnect(): void;
  sendCommand(command: string): boolean;

  on(event: 'connected', listener: () => void): this;
  on(event: 'disconnected', listener: (payload: DisconnectedMessage) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'chunk', listener: (payload: ChunkMessage) => void): this;
  on(event: 'blockUpdate', listener: (payload: BlockUpdateMessage) => void): this;
  on(event: 'entity', listener: (payload: EntityMessage) => void): this;
  on(event: 'entityGone', listener: (payload: EntityGoneMessage) => void): this;
  on(event: 'entityAnimation', listener: (payload: EntityAnimationMessage) => void): this;
  on(event: 'sound', listener: (payload: SoundMessage) => void): this;
  on(event: 'particle', listener: (payload: ParticleMessage) => void): this;
  on(event: 'blockAction', listener: (payload: BlockActionMessage) => void): this;
  on(event: 'blockEntities', listener: (payload: BlockEntitiesMessage) => void): this;
  on(event: 'time', listener: (payload: TimeMessage) => void): this;
  on(event: 'weather', listener: (payload: WeatherMessage) => void): this;
  on(event: 'scoreboard', listener: (payload: ScoreboardMessage) => void): this;
  on(event: 'bossbar', listener: (payload: BossbarMessage) => void): this;
  on(event: 'containerOpen', listener: (payload: ContainerOpenMessage) => void): this;
  on(event: 'containerClose', listener: (payload: ContainerCloseMessage) => void): this;
  on(event: 'containerContent', listener: (payload: ContainerContentMessage) => void): this;
  on(event: 'containerSlot', listener: (payload: ContainerSlotMessage) => void): this;
  on(event: 'biomes', listener: (payload: BiomesMessage) => void): this;
  on(event: 'position', listener: (payload: PositionMessage) => void): this;
  on(event: 'allPositions', listener: (payload: AllPositionsMessage) => void): this;
  on(event: 'status', listener: (payload: StatusMessage) => void): this;
  on(event: 'raw', listener: (payload: RawMessage) => void): this;
  on(event: string, listener: (...args: any[]) => void): this;
}

