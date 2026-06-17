# Modelo Semântico — `msdl_antt`

## Arquivo Power BI

| Arquivo | Descrição |
|---|---|
| `pbix_antt.pbix` | Relatório Power BI conectado ao Modelo Semântico via Direct Lake |

## Como usar

### Opção A — Abrir no Power BI Desktop (modo Direct Lake)

1. Abra o `pbix_antt.pbix` no Power BI Desktop
2. Clique em **Transformar dados** → **Configurações da fonte de dados**
3. Aponte para o `msdl_antt` do seu workspace Fabric
4. Atualize as credenciais com sua conta Microsoft

> O arquivo `.pbix` em modo Direct Lake **não contém dados** — lê diretamente do OneLake. O relatório só funciona com o pipeline executado e o Modelo Semântico publicado no Fabric.

### Opção B — Publicar e usar no Fabric

1. No Power BI Desktop: **Início** → **Publicar** → selecione `workspace-antt`
2. Mova o relatório publicado para a pasta **ModeloSemantico** no workspace
3. Acesse pelo browser em [app.fabric.microsoft.com](https://app.fabric.microsoft.com)

## Medidas DAX incluídas

| Medida | Fórmula |
|---|---|
| `Total Acidentes` | `COUNTROWS( gold_fato_acidente )` |
| `Total Mortos` | `SUM( gold_fato_acidente[mortos] )` |
| `Total Vítimas` | `SUM( gold_fato_acidente[total_vitimas] )` |
| `Acidentes Fatais` | `CALCULATE( COUNTROWS(...), severidade = "Fatal" )` |
| `Taxa Mortalidade %` | `DIVIDE( [Total Mortos], [Total Acidentes], 0 )` |
| `Total Veículos Envolvidos` | `SUM( gold_fato_veiculo_acidente[quantidade] )` |
| `Total Vítimas Detalhado` | `SUM( gold_fato_vitima_acidente[quantidade] )` |

## Relacionamentos configurados

12 relacionamentos do Constellation Schema — ver seção 4 do [README principal](../README.md#4-modelo-de-dados--mer).
