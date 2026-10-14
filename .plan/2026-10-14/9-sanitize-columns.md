# Implementação de Sanitização de Texto e de Colunas do DataFramer

## User Prompt
> Em phynfra/atomic/utils.py adicione a função sanitize(value) que converte o texto para ASCII via unicodedata.normalize('NFKD'), remove acentos, substitui caracteres não alfanuméricos por underline, força minúsculo, unifica múltiplos underlines consecutivos e apara bordas; em phynfra/apache/spark.py importe a função sanitize de phynfra.atomic.utils e adicione na classe DataFramer o método sanitizeColumns(self, dataframe=None) que obtém as colunas de self.dataframe (ou do dataframe informado), descarta colunas cujo nome original seja vazio ou composto apenas por espaços em branco, renomeia as colunas restantes aplicando sanitize em cada uma via withColumnRenamed ou toDF, atualiza self.dataframe em memória e retorna self de forma fluente, respeitando o padrão camelCase e indentação com tabs.

---

## Análise e Requisitos

1. **Função `sanitize(value)` em `phynfra/atomic/utils.py`**:
   - Assinatura: `def sanitize (value: str) -> str:`
   - Normalizar para ASCII via `unicodedata.normalize('NFKD', str(value))`.
   - Remover acentos via `.encode('ascii', 'ignore').decode('ascii')`.
   - Forçar minúsculo via `.lower()`.
   - Substituir caracteres não alfanuméricos por underline `_` via `re.sub(r'[^a-z0-9]+', '_', s)`.
   - Unificar múltiplos underlines consecutivos via `re.sub(r'_+', '_', s)`.
   - Aparar bordas via `.strip('_')`.

2. **Método `DataFramer.sanitizeColumns` em `phynfra/apache/spark.py`**:
   - Importar `sanitize` de `phynfra.atomic.utils`.
   - Assinatura: `def sanitizeColumns (self, dataframe: SparkDataFrame = None) -> 'DataFramer':`
   - Obter DataFrame alvo (`dataframe` se fornecido e válido, senão fallback para `self.dataframe`).
   - Identificar colunas e descartar aquelas cujo nome seja vazio ou composto apenas por espaços em branco (`col and col.strip()`).
   - Renomear as colunas restantes aplicando `sanitize` em cada uma via `toDF(*sanitizedNames)`.
   - Atualizar `self.dataframe` em memória e retornar `self` de forma fluente.

3. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Implementar `sanitize` em `phynfra/atomic/utils.py` [done]
- Adicionar a função `sanitize(value)` com conversão ASCII, remoção de acentos, substituição de não-alfanuméricos, lowercase e strip.

### Step 2: Implementar `DataFramer.sanitizeColumns` em `phynfra/apache/spark.py` [done]
- Importar `sanitize` de `phynfra.atomic.utils`.
- Adicionar o método `sanitizeColumns` na classe `DataFramer` com descarte de colunas vazias, renomeação com `toDF` e retorno fluente.

---

## Análise de Segurança
- **Higienização de Identificadores (Sanitização)**: Elimina caracteres especiais, espaços, acentos e pontuações de nomes de colunas que poderiam causar falhas de parse, SQL injection ou incompatibilidade em engines como Parquet, Delta Lake, Athena ou Glue Catalog.
- **Prevenção de Colunas Fantasmas**: Descarte de colunas com nomes vazios ou puramente de espaços evita corrupção no schema de tabelas Delta.
