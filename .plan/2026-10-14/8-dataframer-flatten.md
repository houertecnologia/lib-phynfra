# Implementação do Método DataFramer.flatten

## User Prompt
> 1 No arquivo phynfra/apache/spark.py, dentro da classe DataFramer, adicione o método flatten(self, columns=None, separator='_', explodeArrays=False, maxDepth=None, dataframe=None) para desaninhamento estruturado em memória sem persistência: importe StructType e ArrayType de pyspark.sql.types e functions como F de pyspark.sql caso ainda não importados; utilize o dataframe passado por argumento ou faça fallback para self.dataframe garantindo que não seja nulo; se columns for informado, restrinja o desaninhamento às colunas listadas (convertendo string única em lista); itere identificando campos StructType desmembrando-os em novas colunas com o formato f'{parentCol}{separator}{childCol}' e descartando a coluna pai original; trate ArrayType com F.explode_outer somente se explodeArrays=True; respeite o limite de iterações caso maxDepth seja definido como número inteiro; atualize self.dataframe com o resultado final, retorne self fluentemente para encadeamento, seguindo rigorosamente a indentação com tabs e nomes em camelCase.

---

## Análise e Requisitos

1. **Imports e Dependências**:
   - `StructType`, `ArrayType` (já presentes em `pyspark.sql.types`).
   - `functions as F` (já presente em `pyspark.sql`).

2. **Assinatura e Tipagem**:
   - `def flatten (self, columns = None, separator: str = '_', explodeArrays: bool = False, maxDepth: int = None, dataframe: SparkDataFrame = None) -> 'DataFramer':`
   - Retorno fluente de `self`.

3. **Validação de Entradas (Guard Clauses)**:
   - Resolver DataFrame alvo (`dataframe` se fornecido e válido, senão fallback para `self.dataframe`); lançar `ValueError` se não existir.
   - Normalizar `columns` caso informado (convertendo string única em lista `[columns]`).
   - Validar `separator` como string.

4. **Lógica de Desaninhamento em Memória**:
   - Loop iterativo respeitando `maxDepth` caso definido.
   - Detecção de campos `StructType` e desmembramento em colunas derivadas no formato `f'{parentCol}{separator}{childCol}'` descartando o pai original.
   - Detecção e tratamento de `ArrayType` via `F.explode_outer(F.col(fieldName))` apenas quando `explodeArrays=True`.
   - Atualização dinâmica das colunas alvo (`activeColumns`) para continuar desaninhando campos derivados da coluna selecionada.
   - Atualizar `self.dataframe` com o DataFrame resultante.
   - Retornar `self` fluentemente.

5. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Implementar o Método `DataFramer.flatten` em `phynfra/apache/spark.py` [done]
- Adicionar o método `flatten` na classe `DataFramer`.
- Substituir o comentário temporário `# flatten :: ...`.

---

## Análise de Segurança
- **Desaninhamento sem Persistência (Em Memória)**: A operação é realizada inteiramente como transformação preguiçosa (lazy evaluation) do Spark DataFrame, sem persistência temporária ou escrita em disco que possa expor dados em repouso.
- **Prevenção de Loops Infinitos**: Respeito estrito ao limite de profundidade `maxDepth` e término natural quando nenhum campo complexo restante for detectado.
- **Tratamento de Dados Nulos**: O uso de `explode_outer` preserva registros nulos ou listas vazias, prevenindo descarte silencioso de linhas.
