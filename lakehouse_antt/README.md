# Lakehouse ANTT — Camada Bronze

## Fonte

Os arquivos CSV são dados **públicos** disponibilizados pela ANTT (Agência Nacional de Transportes Terrestres) sob a [Lei de Acesso à Informação (LAI)](https://www.gov.br/antt/pt-br/acesso-a-informacao/dadosabertos).

- **Portal:** [dados.antt.gov.br](https://dados.antt.gov.br)
- **Dataset:** Demonstrativo de Acidentes por Concessionária
- **Período:** 2007 – 2025
- **Encoding:** Windows-1252 · Separador: `;`

## Estrutura

```
lakehouse_antt/
└── Files/
    └── bronze/
        └── acidentes/
            ├── demostrativo_acidentes_aco.csv          (~1 MB)
            ├── demostrativo_acidentes_af.csv           (~6 MB)
            ├── demostrativo_acidentes_afd.csv          (~16 MB)
            ├── demostrativo_acidentes_als.csv          (~17 MB)
            └── ... (35 arquivos — 1 por concessionária)
```

## Como usar no Microsoft Fabric

Após clonar o repositório, faça upload da pasta `lakehouse_antt/Files/bronze/acidentes/` para o caminho correspondente no OneLake:

```
lakehouse_antt
└── Files/
    └── bronze/
        └── acidentes/
            └── *.csv
```

O notebook `01_nb_ingestao_bronze_acidentes` referencia o caminho `Files/bronze/acidentes` no OneLake.

## Concessionárias incluídas (35)

| Arquivo | Concessionária |
|---|---|
| `*_aco.csv` | ACO |
| `*_af.csv` | AF |
| `*_afd.csv` | AFD |
| `*_als.csv` | ALS |
| `*_aps.csv` | APS |
| `*_arb.csv` | ARB |
| `*_concebra.csv` | Concebra |
| `*_concer.csv` | Concer |
| `*_cro.csv` | CRO |
| `*_eco050.csv` | Eco050 |
| `*_eco101.csv` | Eco101 |
| `*_ecoviasaraguaia.csv` | Ecovias Araguaia |
| `*_ecoviasdocerrado.csv` | Ecovias do Cerrado |
| `*_ecoviascapixaba.csv` | Ecovias Capixaba |
| `*_ecosul.csv` | Ecosul |
| `*_ecoponte.csv` | Ecoponte |
| `*_ecoriominas.csv` | Ecoriominas |
| `*_elovias.csv` | Elovias |
| `*_epr_iguacu.csv` | EPR Iguaçu |
| `*_epr_litoral_pioneiro.csv` | EPR Litoral Pioneiro |
| `*_nova_381.csv` | Nova 381 |
| `*_pantanal.csv` | Pantanal |
| `*_prvias.csv` | PRVias |
| `*_riosp.csv` | RioSP |
| `*_rota-verde-goias.csv` | Rota Verde Goiás |
| `*_trans.csv` | Trans |
| `*_via040.csv` | Via040 |
| `*_viaaraucaria.csv` | Via Araucária |
| `*_viabahia.csv` | Via Bahia |
| `*_viabrasil.csv` | Via Brasil |
| `*_viacosteira.csv` | Via Costeira |
| `*_viacristais.csv` | Via Cristais |
| `*_viamineira.csv` | Via Mineira |
| `*_viasul.csv` | Via Sul |
| `*_way_262.csv` | Way 262 |
