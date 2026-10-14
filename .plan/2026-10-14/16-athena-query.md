# Implementação do Método Athena.query

## User Prompt
> No arquivo phynfra/aws/athena.py, adicione o método query(self, sql, database, results, asDataFramer=True, spark=None, pollInterval=1, timeout=60) para execução síncrona de consultas SQL no Athena: importe time no topo; no método, valide se sql, database e results foram informados como strings válidas; inicie a execução da query via self.client.start_query_execution salvando o QueryExecutionId retornado; faça polling no loop com time.sleep(pollInterval) consultando self.client.get_query_execution até que o status seja 'SUCCEEDED' (levantando RuntimeError com StateChangeReason se 'FAILED' ou 'CANCELLED', e TimeoutError se ultrapassar timeout); quando concluído, caso asDataFramer seja True, monte o caminho do CSV de saída f"{results.rstrip('/')}/{executionId}.csv" e leia diretamente via DataFramer(spark) retornando a instância do DataFramer; caso asDataFramer seja False, extraia os resultados via self.client.get_query_results, monte o cabeçalho a partir da primeira linha e mapeie as linhas de dados seguintes retornando uma lista de dicionários, mantendo padrão camelCase e indentação com tabs.

---

## Análise e Requisitos

1. **Imports em `phynfra/aws/athena.py`**:
   - `import time`

2. **Assinatura e Parâmetros**:
   - `def query (self, sql: str, database: str, results: str, asDataFramer: bool = True, spark = None, pollInterval: int = 1, timeout: int = 60):`

3. **Validação de Entradas (Guard Clauses)**:
   - Validar se `sql`, `database` e `results` são strings não vazias.

4. **Início da Execução**:
   - Chamar `self.client.start_query_execution` com `QueryString`, `Database` e `OutputLocation`.
   - Obter `QueryExecutionId`.

5. **Polling Síncrono com Timeout**:
   - Loop com `time.sleep(pollInterval)`.
   - Obter status via `self.client.get_query_execution`.
   - Se `state == 'SUCCEEDED'`: interromper loop.
   - Se `state in ('FAILED', 'CANCELLED')`: lançar `RuntimeError` com `StateChangeReason`.
   - Se tempo total exceder `timeout`: lançar `TimeoutError`.

6. **Processamento e Retorno de Resultados**:
   - Caso `asDataFramer == True`:
     - Montar caminho CSV: `f'{results.rstrip("/")}/{executionId}.csv'`.
     - Validar `spark`.
     - Retornar `DataFramer(spark).read(csvPath)`.
   - Caso `asDataFramer == False`:
     - Extrair resultados via `self.client.get_query_results`.
     - Montar lista de dicionários mapeando cabeçalhos e valores.
     - Retornar lista de dicionários.

7. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Implementar o Método `Athena.query` em `phynfra/aws/athena.py` [done]
- Importar `time`.
- Implementar `query` na classe `Athena` com polling síncrono e suporte dual (DataFramer ou lista de dicionários).

---

## Análise de Segurança
- **Proteção Contra Hang / Travamento**: O controle de timeout estrito previne que loops infinitos consumam recursos e bloqueiem pipelines em caso de degradação da AWS.
- **Validação de Entrada e SQL Injection**: `sql`, `database` e `results` passam por validação defensiva de formato; a execução ocorre dentro do ambiente de segurança gerenciado pelo Athena / AWS IAM.
