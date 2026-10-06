# Workshop RH — Base de dados para Databricks

Base **100% sintética/fictícia** de People Analytics para o workshop de Genie One, Agents e Dashboards via Genie Code.
Nenhum dado é real: nomes, CPFs, e-mails, salários e políticas foram gerados artificialmente.

## Conteúdo
| Pasta/arquivo | O que é |
|---|---|
| `01_instalar_dados.py` | Notebook que cria tudo no Unity Catalog |
| `dados/` | 12 tabelas em Parquet + `metadados.json` (descrições das colunas e chaves) |
| `politicas_rh/` | 8 políticas de RH fictícias em PDF |

Resultado da instalação: 13 tabelas Delta (a 13ª, `headcount_mensal`, é calculada) com descrições em português,
chaves primárias/estrangeiras e o volume `politicas_rh` com os PDFs.

## Como instalar (≈ 5 min)
1. **Traga a pasta para o workspace**, de uma destas formas:
   - **Git folder:** *Workspace → Create → Git folder* e cole a URL deste repositório; ou
   - **Upload:** descompacte o `.zip` e importe a pasta inteira (*Workspace → Import*), mantendo a estrutura de pastas.
2. Abra `01_instalar_dados`, preencha **Catálogo** e **Schema** nos campos do topo (padrão `workshop.embracon_rh`).
3. Conecte em **Serverless** (ou em qualquer cluster com Unity Catalog) e clique em **Run all**.
4. A última célula confere as contagens de todas as tabelas. Se aparecer tudo como `OK`, terminou.

**Permissões:** `USE CATALOG` + `CREATE SCHEMA` no catálogo escolhido (ou `CREATE TABLE` e `CREATE VOLUME` num schema já existente).
Rodar de novo recria a base do zero.

## Depois da instalação
- **Genie Space:** adicione as 13 tabelas do schema e, para perguntas sobre políticas, o volume `politicas_rh`.
  Para usar os PDFs, ative a preview *Analyze Files in Volumes with Genie Agents* e ligue o **Agent mode** no chat.
- **Dashboards:** use as tabelas diretamente. `headcount_mensal` já traz headcount, admissões e desligamentos por mês.
