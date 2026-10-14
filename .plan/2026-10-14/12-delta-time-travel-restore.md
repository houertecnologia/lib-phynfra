# Suporte a Time Travel e Restauração de Tabelas Delta

## User Prompt
> No arquivo phynfra/apache/spark.py, adicione suporte a time travel e restauração de tabelas Delta: na classe DataFramer, atualize o método read para aceitar os parâmetros opcionais version=None e timestamp=None, mapeando-os internamente nas opções do Spark como 'versionAsOf' e 'timestampAsOf' respectivamente (com precedência de version sobre timestamp caso ambos sejam informados); na classe Spark, importe DeltaTable de delta.tables caso ainda não esteja importado e adicione o método restore(self, destination, version=None, timestamp=None) que valida se destination e ao menos version ou timestamp foram passados (dando precedência a version sobre timestamp), carrega a tabela via DeltaTable.forPath(self.spark, destination), executa deltaTable.restoreToVersion(version) ou deltaTable.restoreToTimestamp(timestamp), e retorna DataFramer(self).read(destination) com a tabela restaurada, mantendo o padrão camelCase e indentação com tabs.

---

## Análise e Requisitos

1. **Atualização de `DataFramer.read`**:
   - Assinatura: `def read (self, fullpath: str, arguments: dict = None, version = None, timestamp = None) -> 'DataFramer':`
   - Parâmetros opcionais `version=None` e `timestamp=None`.
   - Mapeamento nas opções Spark como `'versionAsOf'` e `'timestampAsOf'`.
   - Precedência de `version` sobre `timestamp` caso ambos sejam informados.
   - Retorno fluente de `self`.

2. **Implementação de `Spark.restore`**:
   - Assinatura: `def restore (self, destination: str, version = None, timestamp = None) -> DataFramer:`
   - Validação se `destination` é string não vazia.
   - Validação se ao menos `version` ou `timestamp` foi informado.
   - Precedência de `version` sobre `timestamp`.
   - Carregamento da tabela via `DeltaTable.forPath(self.spark, destination)`.
   - Execução de `deltaTable.restoreToVersion(version)` ou `deltaTable.restoreToTimestamp(timestamp)`.
   - Retorno de `DataFramer(self).read(destination)` com a tabela restaurada.

3. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Atualizar o Método `DataFramer.read` em `phynfra/apache/spark.py` [done]
- Adicionar os parâmetros `version` e `timestamp` no método `read`, configurando `'versionAsOf'` e `'timestampAsOf'`.

### Step 2: Implementar o Método `Spark.restore` em `phynfra/apache/spark.py` [done]
- Adicionar o método `restore` na classe `Spark` para restauração por versão ou timestamp via `DeltaTable`.

---

## Análise de Segurança
- **Recuperação de Desastres e RPO (Time Travel & Restore)**: Capacidade nativa de reverter corrupções de dados acidentais, deleções indevidas ou migrações defeituosas restaurando versões históricas de tabelas Delta de forma atômica.
- **Validação de Entrada**: Bloqueio de chamadas inválidas sem versão ou timestamp definidos, impedindo execuções de restauração ambíguas.
