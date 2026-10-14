# Rastreamento de Exclusões Lógicas (Soft Delete) no Método DataFramer.upsert

## User Prompt
> No arquivo phynfra/apache/spark.py, dentro da classe DataFramer, atualize o método upsert para incorporar o rastreamento opcional de exclusões lógicas (soft delete) adicionando os parâmetros trackDeletes=False e deleteColumn='deleted_at': importe functions como F de pyspark.sql no topo se ainda não houver; na cadeia de merge do DeltaTable, adicione condicionalmente a cláusula .whenNotMatchedBySourceUpdate(condition=f"target.{deleteColumn} IS NULL", set={f"target.{deleteColumn}": F.current_timestamp()}) caso trackDeletes seja True; mantenha o comportamento padrão quando trackDeletes for False, assegure o tratamento de primeira gravação e retorno fluente de self, seguindo rigorosamente a indentação com tabs e nomes em camelCase.

---

## Análise e Requisitos

1. **Imports no Topo de `phynfra/apache/spark.py`**:
   - `from pyspark.sql import functions as F`

2. **Assinatura e Parâmetros**:
   - `def upsert (self, destination: str, primaryKeys, dataframe: SparkDataFrame = None, trackDeletes: bool = False, deleteColumn: str = 'deleted_at') -> 'DataFramer':`
   - Parâmetros opcionais `trackDeletes=False` e `deleteColumn='deleted_at'`.

3. **Cadeia de Merge do DeltaTable**:
   - Inicializar a cadeia de merge:
     `mergeBuilder = deltaTable.alias('target').merge(sourceDataFrame.alias('source'), mergeCondition).whenMatchedUpdateAll().whenNotMatchedInsertAll()`
   - Se `trackDeletes` for `True`:
     `mergeBuilder = mergeBuilder.whenNotMatchedBySourceUpdate(condition = f'target.{deleteColumn} IS NULL', set = {f'target.{deleteColumn}': F.current_timestamp()})`
   - Executar `mergeBuilder.execute()`.

4. **Tratamento de Primeira Gravação e Retorno Fluente**:
   - Manter bloco `except AnalysisException:` para salvar diretamente em caso de tabela inexistente.
   - Retorno de `self`.

5. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores (`trackDeletes`, `deleteColumn`, `mergeBuilder`).
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Atualização dos Imports e do Método `DataFramer.upsert` em `phynfra/apache/spark.py` [done]
- Importar `functions as F` de `pyspark.sql`.
- Atualizar a assinatura de `upsert` com `trackDeletes` e `deleteColumn`.
- Adicionar condicionalmente a cláusula `whenNotMatchedBySourceUpdate` na cadeia do `mergeBuilder`.

---

## Análise de Segurança
- **Integridade Referencial e Auditoria (Soft Delete)**: O rastreamento de exclusões por soft delete preserva o histórico de registros excluídos na fonte sem perda destrutiva de dados, facilitando auditoria e compliance (LGPD/GDPR).
- **Consistência de Timestamp**: O uso de `F.current_timestamp()` garante marcação temporal sincronizada gerada pelo engine Spark.
