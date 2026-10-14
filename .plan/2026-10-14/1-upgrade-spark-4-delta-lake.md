# Upgrade Apache Spark 4.0.0 e Delta Lake (delta-spark)

## User Prompt
> gostei do MEMORY.md. O phynfra hoje está com o spark 3.5 - altere as dependencias para que comecemos a utilziar o spark 4.0.0, incluindo a dependencia de delta-table, delta-spark correspondente a versão 4.0.0. Verifique nas classes do phynfra se há alguam incompatibilidade. Apresente o plano para mudanças para aprovação. Mostre código se precisar altera-lo

---

## Análise de Incompatibilidades

1. **Dependências (`pyproject.toml`)**:
   - `pyspark`: atualmente `>=3.5,<4.0`. Atualizar para `>=4.0.0,<5.0.0`.
   - `delta-spark`: atualmente `*`. Restringir para `>=4.0.0,<5.0.0` para garantir que o delta-spark 4.0.x seja resolvido em sincronia com o Spark 4.0.0 (Delta Lake 4.0.0 é a versão oficial projetada para Spark 4.0.0 com Scala 2.13 e Java 17+).
   - `deltalake`: mantido como `*` (biblioteca leve Rust delta-rs para operações sem JVM/Spark).
   - `version`: elevar versão do pacote para `4.0.0`.

2. **Imports de DataFrame (`phynfra/apache/spark.py`, `phynfra/aws/glue.py`, `phynfra/aws/athena.py`)**:
   - O import `from pyspark.sql.dataframe import DataFrame as SparkDataFrame` aponta para a classe classic interna que no Spark 4 foi reorganizada. No Spark 4.0 (com a unificação do Spark Connect e Classic), a forma universal e compatível com ambos os modos é `from pyspark.sql import DataFrame as SparkDataFrame`.

3. **Correção de Bug em `phynfra/apache/spark.py`**:
   - No método `to_list()`, linha 296, existia uma referência à variável inexistente `instance` (`elif isinstance(instance, PandasDataFrame)`). Deve ser corrigida para `elif isinstance(self.dataframe, PandasDataFrame):`.

4. **EMR Serverless / Airflow (`phynfra/apache/airflow.py`)**:
   - Na função `add_emr_serverless_tasks()`, havia hardcoded `--packages io.delta:delta-core_2.12:2.3.0`.
   - No Spark 4.0.0, Scala 2.12 foi descontinuado e o pacote Delta Lake foi renomeado de `delta-core` para `delta-spark` compilado para Scala 2.13: `io.delta:delta-spark_2.13:4.0.0`. Manter a referência antiga causa falha imediata na submissão de jobs.

5. **Dockerfile e Scripts (`.dakt/docker/spark.Dockerfile`, `scripts/download-jar.sh`, `phynfra/boilerplate/entrypoint.py`)**:
   - Dockerfile de base atualizado de `apache/spark:3.5.0` para `apache/spark:4.0.0`.
   - Download de JARs atualizado de `delta-core_2.12/1.0.0` para `delta-spark_2.13/4.0.0`.

---

## Passos do Plano

### Step 1: Atualização de Dependências em `pyproject.toml` [done]
- Atualizar versão do phynfra para `4.0.0`.
- Atualizar `pyspark = ">=4.0.0,<5.0.0"`.
- Atualizar `delta-spark = ">=4.0.0,<5.0.0"`.

### Step 2: Compatibilização em `phynfra/apache/spark.py` [done]
- Alterar import de DataFrame para `from pyspark.sql import DataFrame as SparkDataFrame`.
- Corrigir checagem de tipo em `to_list` (`self.dataframe`).

### Step 3: Compatibilização em `phynfra/apache/airflow.py` [done]
- Atualizar o pacote Maven padrão do Delta Lake no EMR Serverless para `io.delta:delta-spark_2.13:4.0.0`.

### Step 4: Compatibilização de Imports em `phynfra/aws/glue.py` e `phynfra/aws/athena.py` [done]
- Alterar import de `pyspark.sql.dataframe` para `pyspark.sql`.

### Step 5: Atualização de Docker e Scripts de Suporte [done]
- Atualizar `.dakt/docker/spark.Dockerfile` para imagem base `apache/spark:4.0.0` e JAR `delta-spark_2.13-4.0.0.jar`.
- Atualizar `scripts/download-jar.sh` para baixar `delta-spark_2.13-4.0.0.jar`.
- Atualizar referências de versão em comentários no `phynfra/boilerplate/entrypoint.py`.

---

## Análise de Segurança
- **Injeção e Validação de Entradas**: Nenhuma entrada externa desprotegida é adicionada; as alterações mantêm todas as validações de tipos e parâmetros existentes.
- **Gerenciamento de Segredos e Credenciais**: Nenhuma credencial ou token foi hardcoded ou alterado.
- **Dependências de Terceiros (SCA)**: A atualização para `pyspark 4.0.0` e `delta-spark 4.0.0` remove vulnerabilidades conhecidas presentes nas versões anteriores (Spark 3.5.x e Delta 2.x/3.x legadas).
- **Compatibilidade de Ambiente**: O Spark 4.0 exige Java 17+ e Python 3.9+, prevenindo o uso de runtimes antigos sem suporte de segurança.
