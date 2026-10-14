# Implementação do Método DataFramer.enforce

## User Prompt
> No arquivo phynfra/apache/spark.py, dentro da classe DataFramer, adicione o método enforce(self, schema, fallback='string', dataframe=None) para aplicar contratos de schema com projeção estrita e tolerância a falhas: importe functions como F de pyspark.sql caso ainda não esteja importado; no método, valide se schema é uma lista não vazia de dicionários contendo as chaves 'source', 'target' e 'type'; utilize o dataframe passado por argumento ou faça fallback para self.dataframe garantindo que não seja nulo; para cada item do schema, verifique se a coluna 'source' existe no DataFrame atual (caso não exista, crie-a via F.lit(None)); tente aplicar a expressão de cast F.col(source).cast(targetType).alias(target) e, caso ocorra qualquer exceção de resolução de tipo, capture e aplique o fallback fazendo cast para o tipo string F.col(source).cast(fallback).alias(target); encadeie o select final contendo exclusivamente as colunas 'target' mapeadas, descartando quaisquer colunas não listadas; atualize self.dataframe com o DataFrame projetado e retorne self fluentemente, respeitando a indentação com tabs e nomes em camelCase.

---

## Análise e Requisitos

1. **Imports e Dependências**:
   - `functions as F` (já importado de `pyspark.sql`).

2. **Assinatura e Tipagem**:
   - `def enforce (self, schema: list, fallback: str = 'string', dataframe: SparkDataFrame = None) -> 'DataFramer':`
   - Retorno fluente de `self`.

3. **Validação de Entradas (Guard Clauses)**:
   - Resolver DataFrame alvo (`dataframe` se fornecido e válido, senão fallback para `self.dataframe`); lançar `ValueError` se não existir.
   - Validar se `schema` é uma lista não vazia de dicionários, contendo as chaves obrigatórias `'source'`, `'target'` e `'type'`.

4. **Tratamento de Colunas e Casts**:
   - Para cada entrada no schema, verificar se `source` existe no DataFrame atual; caso não exista, criar a coluna via `currentDataFrame.withColumn(source, F.lit(None))`.
   - Tentar a expressão de cast `F.col(source).cast(targetType).alias(target)`. Caso ocorra exceção de resolução de tipo, aplicar o fallback `F.col(source).cast(fallback).alias(target)`.
   - Projetar o DataFrame final via `.select(*selectExpressions)` contendo exclusivamente as colunas `target` mapeadas, descartando colunas excedentes.
   - Atualizar `self.dataframe` com o DataFrame projetado e retornar `self` fluentemente.

5. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Implementar o Método `DataFramer.enforce` em `phynfra/apache/spark.py` [done]
- Adicionar o método `enforce` na classe `DataFramer`.

---

## Análise de Segurança
- **Contratos de Schema Estritos (Governança de Dados)**: A projeção seletiva impede vazamento de colunas não autorizadas ou campos residuais não mapeados em camadas governadas.
- **Tolerância a Falhas e Resiliência**: Fallback seguro para o tipo configurado (ex: string) previne paradas abruptas em pipelines causadas por tipos malformados, permitindo ingestão íntegra e posterior quarentena/auditoria.
