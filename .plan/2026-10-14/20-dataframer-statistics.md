# Implementação do Método DataFramer.statistics

## User Prompt
> No arquivo phynfra/apache/spark.py, dentro da classe DataFramer, adicione o método statistics(self, metric='empty', columns=None, dataframe=None) para extração de métricas de qualidade de dados: importe functions como F de pyspark.sql e isnan caso ainda não importados; utilize o dataframe passado por argumento ou faça fallback para self.dataframe garantindo que não seja nulo; caso columns seja None, utilize todas as colunas de dataframe.columns (convertendo para lista caso seja string única); obtenha o total de linhas com totalRows = dataframe.count(); caso metric seja 'empty', monte para cada coluna selecionada a contagem agregada de valores nulos, NaN, strings vazias ou literais 'null'/'none' com F.count(F.when(F.col(c).isNull() | (F.isnan(F.col(c)) if dict(dataframe.dtypes)[c] in ['double', 'float'] else F.lit(False)) | (F.trim(F.col(c).cast('string')) == '') | (F.lower(F.col(c).cast('string')).isin('null', 'none')), True)).alias(c); execute a agregação com dataframe.select(*aggExprs).collect()[0].asDict(); monte e retorne um dicionário ordenado no formato {coluna: {'count': countValue, 'percentage': round((countValue / totalRows) * 100, 2) if totalRows > 0 else 0.0}}, levantando ValueError caso a métrica informada não seja suportada, com indentação por tabs e nomes em camelCase.

---

## Análise e Requisitos

1. **Imports e Dependências em `phynfra/apache/spark.py`**:
   - `functions as F` (já importado).
   - `from pyspark.sql.functions import isnan`.

2. **Assinatura e Parâmetros**:
   - `def statistics (self, metric: str = 'empty', columns = None, dataframe: SparkDataFrame = None) -> dict:`

3. **Validação de Entradas**:
   - Resolver DataFrame alvo (`dataframe` ou `self.dataframe`); lançar `ValueError` se não existir.
   - Tratar `columns`: se `None`, usar `list(targetDataFrame.columns)`; se `str`, converter para `[columns]`; se lista/tupla, converter para lista de strings.
   - Validar `metric`: suportar `'empty'`; lançar `ValueError` se não suportada.

4. **Contagem e Agregação**:
   - `totalRows = targetDataFrame.count()`.
   - Para cada coluna `c`, montar expressão:
     `F.count(F.when(F.col(c).isNull() | (isnan(F.col(c)) if dict(targetDataFrame.dtypes)[c] in ['double', 'float'] else F.lit(False)) | (F.trim(F.col(c).cast('string')) == '') | (F.lower(F.col(c).cast('string')).isin('null', 'none')), True)).alias(c)`.
   - Executar `aggResult = targetDataFrame.select(*aggExprs).collect()[0].asDict()`.
   - Montar e retornar `{c: {'count': countValue, 'percentage': round((countValue / totalRows) * 100, 2) if totalRows > 0 else 0.0}}`.

5. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Atualizar Imports e Implementar `DataFramer.statistics` em `phynfra/apache/spark.py` [done]
- Importar `isnan` de `pyspark.sql.functions`.
- Adicionar o método `statistics` na classe `DataFramer`.

---

## Análise de Segurança
- **Qualidade e Observabilidade de Dados**: Mapeia falhas de preenchimento, incompletude e valores sentinelas ('null', 'none', NaN) em pipelines sem persistir dados sensíveis fora da sessão.
- **Eficiência Computacional**: Agregação única no nível do catálogo Spark via `select(*aggExprs)` minimizando shuffles e coletas de rede.
