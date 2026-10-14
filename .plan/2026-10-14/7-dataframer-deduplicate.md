# Implementação do Método DataFramer.deduplicate

## User Prompt
> No arquivo phynfra/apache/spark.py, dentro da classe DataFramer, implemente o método deduplicate(self, primaryKeys, timestampColumn, dataframe=None) para deduplicação determinística baseada na versão mais recente: importe Window de pyspark.sql.window e functions como F de pyspark.sql no topo do arquivo; no método, valide se primaryKeys e timestampColumn foram informados (convertendo primaryKeys em lista se for string única); utilize o dataframe fornecido ou faça fallback para self.dataframe garantindo que não seja None; defina a janela Window.partitionBy(primaryKeys).orderBy(F.col(timestampColumn).desc(), F.col("tiebreak").desc()); aplique sobre o DataFrame adicionando a coluna auxiliar tiebreak com F.monotonically_increasing_id() e rank com F.rank().over(window), filtre por rank == 1 e descarte as colunas auxiliares rank e tiebreak; atualize self.dataframe com o resultado caso dataframe externo não tenha sido passado, retorne self fluentemente e mantenha a formatação com tabs e nomes em camelCase.

---

## Análise e Requisitos

1. **Imports no Topo de `phynfra/apache/spark.py`**:
   - `from pyspark.sql.window import Window`
   - `functions as F` (já importado de `pyspark.sql`).

2. **Assinatura e Tipagem**:
   - `def deduplicate (self, primaryKeys, timestampColumn: str, dataframe: SparkDataFrame = None) -> 'DataFramer':`
   - Retorno fluente de `self`.

3. **Validação de Entradas (Guard Clauses)**:
   - Validar se `primaryKeys` foi informado; normalizar caso seja string para `[primaryKeys]` ou lista de strings; lançar `ValueError` se vazio.
   - Validar se `timestampColumn` é string não vazia.
   - Resolver DataFrame alvo (`dataframe` se fornecido e válido, senão `self.dataframe`); lançar `ValueError` se não existir.

4. **Lógica de Deduplicação Determinística**:
   - Criar especificação de janela:
     `deduplicationWindow = Window.partitionBy(keys).orderBy(F.col(timestampColumn).desc(), F.col('tiebreak').desc())`
   - Aplicar encadeamento em linha única com coluna auxiliar `tiebreak` (`F.monotonically_increasing_id()`), `rank` (`F.rank().over(deduplicationWindow)`), filtrar `rank == 1` e descartar `rank` e `tiebreak`.
   - Se `dataframe is None`: atualizar `self.dataframe` com o DataFrame deduplicado.
   - Retornar `self`.

5. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores (`primaryKeys`, `timestampColumn`, `deduplicationWindow`, `deduplicatedDataFrame`, etc.).
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Atualizar Imports e Implementar `DataFramer.deduplicate` em `phynfra/apache/spark.py` [done]
- Importar `Window` de `pyspark.sql.window`.
- Implementar o método `deduplicate` na classe `DataFramer`.

---

## Análise de Segurança
- **Validação de Entrada**: Validação defensiva de colunas chaves e de timestamp contra injeção e valores nulos ou vazios.
- **Determinismo e Idempotência**: A ordenação composta com `tiebreak` (`monotonically_increasing_id()`) assegura desempate determinístico em registros com o mesmo timestamp exato, evitando resultados inconsistentes entre reexecuções.
