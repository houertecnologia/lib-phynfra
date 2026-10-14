# Implementação do Método DataFramer.upsert (Delta Lake MERGE)

## User Prompt
> No arquivo phynfra/apache/spark.py, dentro da classe DataFramer, adicione o método upsert com a assinatura upsert(self, destination, primaryKeys, dataframe=None) para realizar operação atômica de MERGE em tabelas Delta: importe DeltaTable de delta.tables e AnalysisException de pyspark.sql.utils no topo do arquivo; no método, valide se destination e primaryKeys foram informados (convertendo string única em lista caso necessário); utilize o dataframe passado por argumento ou faça fallback para self.dataframe; tente abrir a tabela existente no destino via DeltaTable.forPath(self._spark, destination) montando a condição de merge com ' AND '.join([f'target.{pk} = source.{pk}' for pk in primaryKeys]) e encadeando .alias('target').merge(source.alias('source'), condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute(); capture AnalysisException para tratar o cenário de primeira gravação (tabela inexistente) executando source.write.format('delta').save(destination); mantenha retorno fluente de self, padrão camelCase de nomenclatura e indentação obrigatória com tabs.

---

## Análise e Requisitos

1. **Imports no Topo de `phynfra/apache/spark.py`**:
   - `from delta.tables import DeltaTable`
   - `from pyspark.sql.utils import AnalysisException`

2. **Assinatura e Tipagem**:
   - `def upsert (self, destination: str, primaryKeys, dataframe: SparkDataFrame = None) -> 'DataFramer':`
   - Retorno fluente de `self`.

3. **Validação de Entradas (Guard Clauses)**:
   - Validar se `destination` é string não vazia.
   - Validar se `primaryKeys` foi informado; normalizar caso seja string única para lista `[primaryKeys]`, ou converter sequências (list/tuple) para lista de strings; lançar `ValueError` se vazio.
   - Resolver DataFrame fonte (`dataframe` se fornecido e válido, senão fallback para `self.dataframe`); lançar `ValueError` se não existir.

4. **Operação Atômica de MERGE**:
   - Construir a condição: `' AND '.join([f'target.{pk} = source.{pk}' for pk in keys])`.
   - Tentar abrir via `DeltaTable.forPath(self._spark, destination)`.
   - Executar merge encadeado:
     `deltaTable.alias('target').merge(sourceDataFrame.alias('source'), mergeCondition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`.

5. **Tratamento de Tabela Inexistente (Primeira Carga)**:
   - Capturar `AnalysisException` lançada quando o caminho ainda não é uma Delta Table válida.
   - Executar fallback de primeira gravação: `sourceDataFrame.write.format('delta').save(destination)`.

6. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e métodos.
   - Linhas em branco em corpos de blocos conforme o `gemini.md`.

---

## Passos do Plano

### Step 1: Adicionar Imports e Implementar `DataFramer.upsert` em `phynfra/apache/spark.py` [done]
- Importar `DeltaTable` de `delta.tables` e `AnalysisException` de `pyspark.sql.utils`.
- Implementar o método `upsert` na classe `DataFramer`.

---

## Análise de Segurança
- **Validação de Entrada**: Verificação defensiva de `destination` e colunas de `primaryKeys`, garantindo que não sejam vazios ou formatos inesperados.
- **Atomicidade e Integridade (ACID)**: A operação de MERGE do Delta Lake é atômica e garante isolamento serializável, evitando inconsistências de concorrência ou duplicação de dados.
- **Tratamento de Exceções**: A captura específica de `AnalysisException` impede mascaramento indevido de outros erros do sistema (ex.: falhas de permissão de rede, segredos ausentes ou erros de sintaxe).
