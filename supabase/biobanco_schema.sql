create table if not exists public.voluntarios (
    id_voluntario text primary key,
    expediente text,
    fecha_toma1 date,
    apellido_paterno text,
    apellido_materno text,
    nombre text,
    genero text,
    residencia text,
    fecha_nacimiento date,
    edad integer,
    peso numeric,
    estatura numeric,
    tubos_amarillos integer,
    tubos_verdes integer,
    correo text,
    telefono text,
    patologias text,
    observaciones text,
    created_at timestamptz not null default now()
);

create table if not exists public.visitas (
    id_visita bigint generated always as identity primary key,
    id_voluntario text not null references public.voluntarios(id_voluntario) on delete cascade,
    tipo_toma text not null,
    fecha_programada date,
    fecha_real date,
    estado text,
    created_at timestamptz not null default now()
);

create table if not exists public.racks (
    id_rack text primary key,
    tipo_banco text not null,
    capacidad integer not null,
    ocupadas integer not null default 0,
    created_at timestamptz not null default now()
);

create table if not exists public.alicuotas_suero (
    id_alicuota bigint generated always as identity primary key,
    id_voluntario text not null references public.voluntarios(id_voluntario) on delete cascade,
    tipo_toma text,
    numero_alicuota integer not null,
    fecha_ingreso date,
    id_rack text not null references public.racks(id_rack),
    fila integer not null,
    columna integer not null,
    created_at timestamptz not null default now()
);

create table if not exists public.alicuotas_pbmc (
    id_pbmc bigint generated always as identity primary key,
    id_voluntario text not null references public.voluntarios(id_voluntario) on delete cascade,
    tipo_toma text,
    numero_alicuota integer not null,
    conteo_celular text,
    fecha_ingreso date,
    id_rack text not null references public.racks(id_rack),
    fila integer not null,
    columna integer not null,
    created_at timestamptz not null default now()
);

create index if not exists idx_visitas_voluntario on public.visitas (id_voluntario);
create index if not exists idx_alicuotas_suero_voluntario on public.alicuotas_suero (id_voluntario);
create index if not exists idx_alicuotas_pbmc_voluntario on public.alicuotas_pbmc (id_voluntario);
create index if not exists idx_racks_tipo on public.racks (tipo_banco);
