# Implementação do Módulo PostgreSQL e Método query

## User Prompt
> Na classe PostgreSQL (a ser incluída no Phynfra em phynfra/database/postgresql.py), implemente o método query(self, sql, params=None, asDataFramer=False, spark=None) com tratamento robusto de execução e suporte a DataFramer: importe DataFramer de phynfra.apache.spark; no método, garanta que self.connection esteja ativa (chamando self.connect() se for nula ou estiver fechada); abra um cursor com self.open(); execute cursor.execute(sql, params); verifique se cursor.description existe (indicando consulta com retorno SELECT): caso não exista, retorne cursor.rowcount com a quantidade de linhas afetadas; caso exista, extraia os nomes das colunas de [col[0] for col in cursor.description], faça fetchall() mapeando cada linha em um dicionário dict(zip(columns, row)); caso asDataFramer seja False, retorne a lista de dicionários; caso asDataFramer seja True, valide a sessão do Spark (do argumento ou via Spark) e retorne uma instância de DataFramer carregada com os dados; garanta o fechamento do cursor no bloco finally e capture exceções levantando RuntimeError com detalhes da falha na query, mantendo nomes em camelCase e indentação com tabs.

---

## Análise e Requisitos

1. **Estrutura de `phynfra/database/postgresql.py`**:
   - `phynfra/database/__init__.py`.
   - `phynfra/database/postgresql.py`.
   - Importar `DataFramer` e `Spark` de `phynfra.apache.spark`.
   - Importar `StructType`, `StructField`, `StringType` de `pyspark.sql.types`.

2. **Classe `PostgreSQL`**:
   - `__init__(self, host, database, user, password, port=5432, ...)`: inicializa credenciais e `self.connection = None`.
   - `connect(self)`: conecta via driver PostgreSQL (`psycopg2` / `psycopg`).
   - `open(self)`: verifica se a conexão está ativa (reconectando se fechada) e abre um cursor.
   - `close(self)`: fecha a conexão de forma segura.

3. **Método `query`**:
   - Assinatura: `def query (self, sql: str, params = None, asDataFramer: bool = False, spark = None):`
   - Validação de `sql`.
   - Garantir que `self.connection` esteja ativa via `self.connect()`.
   - Abrir cursor com `self.open()`.
   - Executar `cursor.execute(sql, params)` (ou sem params se None).
   - Se `cursor.description is None`:
     - Executar commit se aplicável.
     - Retornar `cursor.rowcount`.
   - Se `cursor.description` existe:
     - Obter colunas: `columns = [col[0] for col in cursor.description]`.
     - Fazer `fetchall()` e mapear linhas: `[dict(zip(columns, row)) for row in rawRows]`.
     - Se `asDataFramer is False`: retornar lista de dicionários.
     - Se `asDataFramer is True`: validar sessão Spark (argumento ou via `Spark`), criar Spark DataFrame, atribuir a `dataFramer.dataframe` e retornar instância de `DataFramer`.
   - Bloco `finally`: garantir fechamento do cursor via `cursor.close()`.
   - Bloco `except`: capturar exceções e lançar `RuntimeError` com mensagem detalhada.

4. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Criar o Pacote `phynfra/database/` e o Arquivo `phynfra/database/postgresql.py` [done]
- Criar `phynfra/database/__init__.py`.
- Criar `phynfra/database/postgresql.py` contendo a classe `PostgreSQL` e o método `query`.

---

## Análise de Segurança
- **Gerenciamento de Cursores e Conexões**: Fechamento rigoroso de cursores em bloco `finally`, prevenindo vazamento de recursos ou cursores órfãos no banco de dados.
- **Parametrização de Queries**: Suporte a tupla/lista/dicionário em `params` permitindo prevenção de SQL Injection via queries parametrizadas.
