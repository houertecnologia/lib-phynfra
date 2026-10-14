# Refatoração do Método DataFramer.write

## User Prompt
> No arquivo phynfra/apache/spark.py, dentro da classe DataFramer, refatore o método write para unificar gravações simples e particionadas suportando Delta Lake por padrão: adicione os parâmetros opcionais mode='append' e partitions=None (mantendo argumentos fullpath, arguments=None, dataframe=None), faça o tratamento do fullpath via urlparse para extrair extensão removendo barras finais; determine o formato priorizando chave 'format' de arguments, em seguida a extensão do arquivo e usando 'delta' como fallback caso não haja extensão; trate as colunas de partição na variável partitionColumns (suportando argumento partitions ou chaves 'partitions'/'partition_columns' em arguments, convertendo string única para lista); encadeie format(fileFormat).mode(mode).options(**args) e aplique .partitionBy(*partitionColumns) antes do .save(fullpath), mantendo o retorno fluente de self e seguindo o padrão de nomenclatura camelCase e indentação com tabs do projeto

---

## Análise e Requisitos

1. **Parâmetros e Assinatura**:
   - `def write (self, fullpath: str, arguments: dict = None, dataframe: SparkDataFrame = None, mode: str = 'append', partitions = None, overwrite: bool = None) -> 'DataFramer':`
   - Parâmetros opcionais `mode='append'` e `partitions=None`.
   - Compatibilidade preservada para `overwrite`: se informado como booleano (`True` -> `mode='overwrite'`, `False` -> `mode='append'`).

2. **Tratamento do `fullpath` via `urlparse`**:
   - Remover barras finais via `.rstrip('/')`.
   - Extrair a extensão via `urlparse` e `os.path.splitext`.

3. **Determinação do Formato (`fileFormat`)**:
   - 1º Prioridade: chave `'format'` em `arguments`.
   - 2º Prioridade: extensão extraída do arquivo (sem ponto inicial).
   - 3º Prioridade / Fallback: `'delta'`.

4. **Tratamento de Partições (`partitionColumns`)**:
   - Verificar argumento `partitions`.
   - Caso `None`, buscar em `arguments` pelas chaves `'partitions'` ou `'partition_columns'`.
   - Se for `str`, converter para lista com elemento único `[partitions]`.
   - Se for `list` ou `tuple`, converter para `list(partitions)`.
   - Caso contrário, definir como lista vazia `[]`.

5. **Encadeamento e Execução**:
   - Obter DataFrame de destino (`dataframe` se fornecido, senão `self.dataframe`).
   - Obter opções restantes de `arguments` (removendo `'format'`, `'partitions'`, `'partition_columns'`).
   - Encadeamento: `writer = targetDataFrame.write.format(fileFormat).mode(mode).options(**writeOptions)`
   - Se `partitionColumns` estiver preenchida: `writer = writer.partitionBy(*partitionColumns)`
   - Executar `.save(fullpath)`
   - Retorno fluente de `self`.

6. **Estilo e Convenções**:
   - Nomenclatura camelCase para variáveis e identificadores.
   - Indentação com Tabs (tamanho 4).
   - Aspas simples para strings.
   - Linhas em branco nos blocos conforme o `gemini.md`.

---

## Passos do Plano

### Step 1: Refatoração do Método `DataFramer.write` em `phynfra/apache/spark.py` [done]
- Atualizar a assinatura do método `write`.
- Implementar validação de entrada e guard clauses.
- Implementar extração de extensão com `rstrip('/')` e resolução de formato com fallback `'delta'`.
- Implementar resolução e normalização de `partitionColumns`.
- Implementar encadeamento fluente com `.format()`, `.mode()`, `.options()`, `.partitionBy()` e `.save()`.
- Garantir retorno de `self`.

---

## Análise de Segurança
- **Validação de Caminhos e Entradas**: Validação explícita de `fullpath` e verificação da existência de DataFrame válido para gravação.
- **Prevenção de Injeções / Inconsistências**: Tratamento defensivo de parâmetros de partição e extração segura de argumentos, evitando repasse de chaves estruturais inválidas para as opções nativas do Spark.
- **Rastreabilidade e Tratamento de Erros**: Erros de gravação são capturados e propagados com contexto claro.
