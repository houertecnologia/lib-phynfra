# Implementação de Métodos Genéricos de Conversão: convertFrom e convertTo

## User Prompt
> No arquivo phynfra/apache/spark.py, dentro da classe DataFramer, substitua os métodos to_pandas e from_pandas por convertFrom e convertTo com suporte genérico a múltiplos formatos: importe TimestampType, LongType, IntegerType, DoubleType, FloatType, StringType, StructField e StructType de pyspark.sql.types caso ainda não importados; implemente convertFrom(self, dataframe, origin='pandas', intConstant=-1234567) validando se origin é 'pandas'; trate a compatibilidade com Pandas 2+ e backend PyArrow inspecionando os tipos dtypes do pandas, mapeando tipos como int64/timestamp/float para StructFields equivalentes do Spark, substituindo nulos de inteiros por intConstant, convertendo em lista de tuplas com dataframe.values.tolist(), gerando o Spark DataFrame via self._spark.createDataFrame(data, schema=schema) e aplicando .replace(intConstant, None); atualize self.dataframe e retorne self fluentemente; implemente convertTo(self, target='pandas') com alias to(self, target='pandas') que verifica se target é 'pandas', converte self.dataframe via .toPandas() e retorna o resultado; mantenha indentação obrigatória com tabs e padrão camelCase.

---

## Análise e Requisitos

1. **Imports e Dependências**:
   - `TimestampType`, `LongType`, `IntegerType`, `DoubleType`, `FloatType`, `StringType`, `StructField`, `StructType` (já presentes em `pyspark.sql.types`).

2. **Implementação de `convertFrom`**:
   - Assinatura: `def convertFrom (self, dataframe: PandasDataFrame, origin: str = 'pandas', intConstant: int = -1234567) -> 'DataFramer':`
   - Validação de `origin == 'pandas'` e tipo de `dataframe` como `PandasDataFrame`.
   - Compatibilidade com Pandas 2+ / PyArrow:
     - Inspecionar `dataframe.dtypes`.
     - Mapear tipos inteiros (`int64`, `int32`), `float`/`double`, `datetime`/`timestamp` e fallback para `StringType`.
     - Tratar nulos em colunas inteiras substituindo por `intConstant` via `.fillna(intConstant)`.
     - Converter dados com `dataframe.values.tolist()`.
     - Gerar Spark DataFrame via `self._spark.createDataFrame(data, schema=schema)`.
     - Aplicar `.replace(intConstant, None)` para restaurar valores nulos.
     - Atualizar `self.dataframe` e retornar `self` fluentemente.

3. **Implementação de `convertTo` e Alias `to`**:
   - Assinatura: `def convertTo (self, target: str = 'pandas') -> PandasDataFrame:`
   - Assinatura: `def to (self, target: str = 'pandas') -> PandasDataFrame:`
   - Validar `target == 'pandas'`.
   - Validar existência de `self.dataframe`.
   - Converter via `self.dataframe.toPandas()` e retornar o resultado.

4. **Remoção de Métodos Legados**:
   - Substituição de `to_pandas` e `from_pandas`.

5. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Substituir `to_pandas` e `from_pandas` por `convertFrom`, `convertTo` e `to` em `phynfra/apache/spark.py` [done]
- Remover os métodos `to_pandas` e `from_pandas`.
- Implementar `convertFrom` com suporte a Pandas 2+, PyArrow, StructType e intConstant.
- Implementar `convertTo` e seu alias `to`.

---

## Análise de Segurança
- **Compatibilidade de Tipos e Integridade**: Prevenção de corrupção de tipos inteiros e overflow na interoperabilidade entre Pandas e Spark.
- **Validação Antecipada**: Validação explícita de `origin` e `target` garantindo que formatos não suportados sejam rejeitados com clareza.
