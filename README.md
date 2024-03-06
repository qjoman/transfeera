# Analytics Engineer - Transfeera
## Tecnologias
O projeto inicialmente seria desenvolvido em pyspark/Jupyter, mas por questão de gestão de tempo optei por utilizar um ambiente já pronto para uso: Azure Databricks. Apesar do custo elevado, escolhendo as opções corretas foi possível utilizar o produto com custos quase nulos. 

Uma opção utilizada inicialmente foi o Databricks Community mas infelizmente ele é bem limitado na forma de como armazenar dados e clusters constantemente precisam ser recriados graças à limitação de spots para usuários de forma gratuita.

A principal linguagem é Python com PySpark e algumas área em SQL (ETL).

## Modelo de dados
O modelo original é bem simples, tendo apenas 3 entidades: cliente, contrato e transação.

![Modelo original](/imagens/Origem_Transfeera.png)

Transação é um registro de transação financeira de um tipo específico. Um cliente pode fazer qualquer número de transações em um dado dia ou até mesmo nenhuma transação. Esse limite superior aberto chama atenção, porque analisar dados de um cliente que realiza milhares de operações (ou mais) num dia, por exemplo, tem um custo computacional muito alto.

É preciso destacar que a transação não tem dados de fatura, esses dados ficando apenas dentro do contrato. A relação de cliente e contrato me levantou outra questão: um cliente tem somente um contrato? 

Pensando nos pontos acima, proponho a seguinte estrutura.

![Modelo novo](/imagens/DW_Transfeera.png)

Nesse modelo adotei as práticas da modelagem de star schema, onde a transação é o fato central **fact_transacao** e as dimensões são cliente e contrato. Até aqui, não há mudanças no modelo, além da introdução do contrato na transação. Porém, caso o cliente mude o contrato válido, transações no passado ainda estarão ligadas ao antigo contrato, sem necessidade de queries complexas. 

```
Table fact_transacao{
  id_cliente BIGINT
  id_transacao BIGINT
  tipo_transacao STRING
  valor_transacao DOUBLE
  data_movimentacao DATE
  hora_movimentacao TIMESTAMP
  id_contrato BIGINT
}
```

A segunda alteração foi inclusão da tabela de sumário **fact_transacao_dia** para sumários diários de transações. Ela será alimentada uma vez por dia com dados de transação do dia anterior já computados, permitindo realizar queries de maneira muito mais simples. 

Essa tabela não possui qualquer ligação com o horário de transações, se o nível de granulidade da busca for nesse nível, utilize a tabela **fact_transacao**.

```
Table fact_transacoes_dia{
    id_cliente BIGINT
    data_movimentacao DATE
    total_movimentacao DOUBLE
    total_boleto DOUBLE
    total_payin DOUBLE
    total_payout DOUBLE
    quantidade_movimentacoes INT
    quantidade_boleto INT
    quantidade_payout INT
    quantidade_payin INT
    id_contrato BIGINT
}
Table dim_tempo{
  data DATE
  ano INT
  mes INT
  dia INT
  dia_da_semana INT
  dia_do_ano INT
  trimestre INT
}
Table dim_cliente{
  id_cliente BIGINT
  razao_social STRING
  data_inicio_relacionamento DATE
  status STRING
  data_fim_relacionamento DATE
  segmento STRING
}

Table dim_contrato{
  id_contrato BIGINT
  id_cliente BIGINT
  taxa_payout DOUBLE
  taxa_payin DOUBLE
  taxa_boleto DOUBLE
  data_vencimento DATE
}
```

Por fim, foi adicionado a dimensão de tempo **dim_tempo**. Tempo é utilizado como dimensão em star schemas por facilitar buscas em datas de maneira a ser independente da tecnologia do banco e permitir relatórios mais limpos sem grande dificuldade ao disponibilizar `INT` ao invés de objetos `DATE/DATETIME/TIMESTAMP`.

Um exemplo de query utilizando a `dim_tempo`:
```
# todas as transações entre dia 5 e 20 do mês 4
SELECT 
    * 
FROM 
    dw.fact_transacoes_dias ft
JOIN 
    dw.dim_tempo dt
ON 
    ft.data_movimentacao = dt.data
WHERE
    dt.mes = 4 AND dia > 5 AND dia < 20
```

Fatos são denotados pela nomeclatura `fact_NOME_DO_FATO` e dimensões como `dim_NOME_DA_DIMENSAO`. Além das tabelas citadas acima, ainda há **dim_contrato** e **dim_cliente**. Além da troca de nome de algumas colunas (para padronização), não há grandes alterações nessas tabelas.


# ETL
O processo de ETL no sistema deve acontecer em 3 momentos distintos:
- Boostrap, criando o schema do modelo final.
- A cada intervalo de tempo a ser definido.
- No primeiro minuto de cada dia.

## Bootstrap 

Este (notebook)[/Transfeera+-+0Notebooks/ETL:boostrap.py] deve ser utilizado manualmente somente uma vez. Os passos são descritos abaixo.
1) Criar um ambiente de staging onde os dados novos serão armazenados.
2) Criar o modelo final como descrito acima.
3) Carregar os dados na área de staging.
4) Popular as tabelas do modelo final utilizando dados de staging.
5) Popular **dim_tempo** usando a maior e menor data dos dados de transação.
6) Computar **fact_transacoes_dia**.
7) Limpar área de staging.

## Atualizar por intervalo de tempo

Similar ao processo de bootstrap, mas utilizando Databricks Workflows para definição de CRON job. O diagrama abaixo descreve o processo visualmente como presente no job. Note que o código dos notebooks é a descrição dos passos acima.

![diagrama de tarefas para atualização](/imagens/etl.png)

Existe a possibilidade de execução contínua mas o custo financeiro para esse tipo de operação é alto e não foi apresentado o tipo de orçamento pro projeto, logo, o modo scheduled (CRON job) deve ser considerado a melhor opção.

## Atualização de fact_transacoes_dia

Executa somente usando dados do banco local, sem execução de notebook para conexão com bancos externos. Abaixo é possível ver a tela de configuração da task. No lado direito, o horário agendado para execução.
![diagrama de tarefas para atualização](/imagens/etl_fact_transacoes_dia.png)

O código responsável por inserir dados nesta tabela é relativamente simples, apesar de um pouco extenso.
```
%sql
  MERGE INTO dw.fact_transacoes_dia AS fact_t_d USING (
    SELECT
      st.id_cliente,
      data_movimentacao,
      id_contrato,
      COUNT(st.id_cliente) as quantidade_movimentacoes,
      SUM(CASE WHEN tipo_transacao = 'payin' THEN 1 ELSE 0 END ) AS quantidade_payin,
      SUM(CASE WHEN tipo_transacao = 'payout' THEN 1 ELSE 0 END ) AS quantidade_payout,
      SUM(CASE WHEN tipo_transacao = 'boleto' THEN 1 ELSE 0 END ) AS quantidade_boleto,
      SUM(valor_transacao) as total_movimentacao,
      SUM(CASE WHEN tipo_transacao = 'payin' THEN valor_transacao ELSE 0 END) AS total_payin,
      SUM(CASE WHEN tipo_transacao = 'payout' THEN valor_transacao ELSE 0 END ) AS total_payout,
      SUM(CASE WHEN tipo_transacao = 'boleto' THEN valor_transacao ELSE 0 END ) AS total_boleto
    FROM
      staging.transacoes st
      JOIN staging.contratos sc ON sc.id_cliente = st.id_cliente
    WHERE
      st.data_movimentacao <= sc.data_vencimento_contrato
      AND sc.data_vencimento_contrato = (
        SELECT
          MAX(data_vencimento_contrato)
        FROM
          dw.dim_contratos
        WHERE
          id_cliente = st.id_cliente
          AND data_vencimento_contrato >= st.data_movimentacao
      )
    GROUP BY
      data_movimentacao,
      st.id_cliente,
      sc.id_contrato
  ) AS stg_t_d ON fact_t_d.id_cliente = stg_t_d.id_cliente
  AND fact_t_d.data_movimentacao = stg_t_d.data_movimentacao
  WHEN NOT MATCHED THEN
INSERT
  *;
```

1) Agrupar dados de transação por `id_cliente`,`data_movimentacao` e `id_contrato`

```
  GROUP BY
    data_movimentacao,
    st.id_cliente,
    sc.id_contrato
```
2) Realizar as contagens/somas conforme o tipo de transação
```
    COUNT(st.id_cliente) as quantidade_movimentacoes,
    SUM(CASE WHEN tipo_transacao = 'payin' THEN 1 ELSE 0 END ) AS quantidade_payin,
    SUM(CASE WHEN tipo_transacao = 'payout' THEN 1 ELSE 0 END ) AS quantidade_payout,
    SUM(CASE WHEN tipo_transacao = 'boleto' THEN 1 ELSE 0 END ) AS quantidade_boleto,
    SUM(valor_transacao) as total_movimentacao,
    SUM(CASE WHEN tipo_transacao = 'payin' THEN valor_transacao ELSE 0 END) AS total_payin,
    SUM(CASE WHEN tipo_transacao = 'payout' THEN valor_transacao ELSE 0 END ) AS total_payout,
    SUM(CASE WHEN tipo_transacao = 'boleto' THEN valor_transacao ELSE 0 END ) AS total_boleto
```
3) Procurar o último contrato válido do período das transações em questão
```
WHERE
    st.data_movimentacao <= sc.data_vencimento_contrato
    AND sc.data_vencimento_contrato = (
    SELECT
        MAX(data_vencimento_contrato)
    FROM
        dw.dim_contratos
    WHERE
        id_cliente = st.id_cliente
        AND data_vencimento_contrato >= st.data_movimentacao
    )
```

## Considerações sobre ETL

Dois pontos ficaram em aberto nesta etapa:
1) Modificar os notebooks de consulta de dados de bancos externos para incluir a janela de busca. Por exemplo: executar a cada 2 minutos, buscando as transações dessa janela (incluindo cliente e contratos).
2) Desenvolver mecanismos de recuperação de falhas. Se uma conexão falhar, a próxima irá consumir dados da sua janela, ignorando os dados ausentes pela falha.


# Análise de dados

## Considerações iniciais
Sempre que algum dado for referenciado e houver algum arquivo CSV disponível, eles estão presentes no diretório `/dados/` e serão referenciados com links. A diferença entre muitos dos gráficos abaixo e esses dados é a aplicação de filtros sobre o cliente, pois é impraticável visualizar volumes de transação por tipo por cliente (são mais de 100 clientes).

## Análise de volume

Para ter acesso ao volume de transações, a tabela fact_transacoes_dias já possui boa parte do necessário. O código abaixo mostra que apenas uma simples soma dos campos de quantidade permite ter o volume de transação por dia de todos os clientes.

```
faturamento = (
    fact_transacoes_dia_df.join(
        dim_tempo_df,
        fact_transacoes_dia_df["data_movimentacao"] == dim_tempo_df["data"],
    )
    .orderBy("dim_tempo.data", "id_cliente")
)

transacoes_do_mes = (
    faturamento.select(
        "id_cliente", "data", "quantidade_boleto", "quantidade_payout", "quantidade_payin"
    )
    .groupBy("data", "id_cliente")
    .agg(
        sum("quantidade_boleto").alias("quantidade_boleto"),
        sum("quantidade_payout").alias("quantidade_payout"),
        sum("quantidade_payin").alias("quantidade_payin"),
    )
)
```
Filtrando pelo id_cliente 1, temos os gráficos abaixo. Foram adicionadas colunas verticais para demarcar os domingos, bem como uma linha de média móvel de 5 dias.

|![Volume boleto cliente id 1](/imagens/volume_boleto_diaro_cliente_1.png)|
|:--:| 
|![Volume payin cliente id 1](/imagens/volume_payin_diaro_cliente_1.png)|
|![Volume payout cliente id 1](/imagens/volume_payout_diaro_cliente_1.png)|
| *[dados](/dados/volume_tipo_transacao.csv)* |

Notando as transações altas nas primeiras semanas de Novembro/Dezembro, presumi uma tendência de crescimento pela época do ano mas não poderia simplesmente assumir isso. Resolvi fazer a quebra de transações por segmento dos clientes.

|![Volume por segmento](/imagens/transacoes_segmentos.png)|
|:--:| 
| *[dados](/)* |

Novamente, incluí o domingo no gráfico e ficou mais evidente com outros clientes a tendência ao aumento. A maneira mais simples de confirmar seria então com regressão linear. PySpark possui uma lib pra modelos de ML com uma função para criar modelos de regressão. Uma pequena função genérica foi feita para facilitar a criação dos gráficos, por isso todos eles têm a mesma aparência. Todos eles foram alimentados com training set usando todos os valores da série.

|![RL de varejo](/imagens/rl_transacoes_varejo.png)| 
|:--:| 
| Root Mean Squared Error (RMSE): 27.37577572332663 |
| *[dados](/dados/rl_transacoes_varejo.csv)* |

|![RL de Financeiro](/imagens/rl_transacoes_financeiro.png)| 
|:--:| 
| Root Mean Squared Error (RMSE): 17.353604084813956 |
| *[dados](/dados/rl_transacoes_financeiro.csv)* |

|![RL de Industria](/imagens/rl_transacoes_industria.png)| 
|:--:| 
| Root Mean Squared Error (RMSE): 46.34172691276101 |
| *[dados](/dados/rl_transacoes_industria.csv)* |

|![RL de Tecnologia](/imagens/rl_transacoes_tecnologia.png)| 
|:--:| 
| Root Mean Squared Error (RMSE): 53.57800522778191 |
| *[dados](/dados/rl_transacoes_tecnologia.csv)* |

|![RL de Serviços](/imagens/rl_transacoes_servicos.png)| 
|:--:| 
| Root Mean Squared Error (RMSE): 11.951454440867437 |
| *[dados](/dados/rl_transacoes_servicos.csv)* |

Apesar de visualmente serem perceptíveis as têndencias das transações, não posso afirmar que essa seja a abordagem correta, em especial por não conseguir validar os modelos.
Existem validadores de modelo dentro da PySpark que poderiam me informar com mais detalhes, mas a única métrica que consigo trabalhar no momento é RMSE, presente abaixo de cada gráfico.

## Análise de uso por cliente

Não há muito aqui, uma vez que se considerarmos que transação ocorre quando há uso do produto, já temos o uso ou não do sistema.

Para testar o caso de gaps de uso (os dados fornecidos tem todos os dias preenchidos), apliquei um filtro de data que incluía data fora do limite dos dados existentes e assumi que todo dia não havia transação (causados pelo filtro ), não havia uso do produto. Por fim, incluí o valor total movido na data em questão, achei que ao visualizar tanto o uso quanto o valor movido, talvez houvesse alguma relação ainda não vista.

![Uso do sistema, simulando gaps](/imagens/usos_por_dia.png)

Talvez seja interessante incluir aqui a sazonalidade por cliente.

## Faturamento por cliente

Faturamento do cliente é, novamente, relativamente simples. A `fact_transacoes_dia` já possui os valores do dia por tipo e também possui o id do contrato relativo a transacao (considerei que pode haver dados no passado com outros contratos e taxas). Um `join` entre contrato e `fact_transacoes_di` já traz todas as informações relevantes, sendo necessário apenas multiplicar as taxas pelos valores.

```
transacoes_df = (
    (
        fact_transacoes_dia_df
        .join(
            dim_contratos_df,
            dim_contratos_df["id_contrato"] == fact_transacoes_dia_df["id_contrato"],
        )
        .join(
            dim_tempo_df,
            fact_transacoes_dia_df["data_movimentacao"] == dim_tempo_df["data"],
        )
        .withColumn("faturamento_boleto", col("taxa_boleto") * col("total_boleto"))
        .withColumn("faturamento_payin", col("taxa_payin") * col("total_payin"))
        .withColumn("faturamento_payout", col("taxa_payout") * col("total_payout"))
    )
    .groupBy("data", "fact_transacoes_dia.id_cliente")
    .agg(
        sum("faturamento_boleto").alias("faturamento_boleto"),
        sum("faturamento_payin").alias("faturamento_payin"),
        sum("faturamento_payout").alias("faturamento_payout"),
        sum("total_boleto").alias("total_boleto"),
        sum("total_payin").alias("total_payin"),
        sum("total_payout").alias("total_payout"),
    )
    .orderBy("data")
)
```

 Neste primeiro gráfico temos os valores totais movidos conforme os tipos de transação. Novamente, um crescimento no começo novembro até dezembro.

|![valores totais](/imagens/valores_diario_tipos_cliente_1.png)| 
|:--:| 
|*[dados](/dados/valores_dia_tipo.csv)*|

Abaixo, temos a demonstração gráfica do faturamento no período. A maior contribuição vem de boletos e payouts, payin dificilmente chegando à metade dos valores de payout.

|![valor faturamento](/imagens/valores_faturamento_diario_tipos_cliente_1.png)| 
|:--:| 
|*[dados](/dados/valores_dia_tipo.csv)*|

Novamente, RL pode ajudar a visualizar as tendências.

|![RL faturamento boleto](/imagens/rl_faturamento_boleto.png)| 
|:--:|
|  Root Mean Squared Error (RMSE): 243115.9640042144 |
|*[dados](/dados/rl_faturamento_boleto.csv)*|

|![RL faturamento payin](/imagens/rl_faturamento_payin.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 79955.52841091111 |
|*[dados](/dados/rl_faturamento_payin.csv)*|

|![RL faturamento payin](/imagens/rl_faturamento_payout.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 148623.08269192124 |
|*[dados](/dados/rl_faturamento_payout.csv)*|

|![RL faturamento total](/imagens/rl_faturamento_total.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 412704.1750562845 |
|*[dados](/dados/rl_faturamento_total.csv)*|

Usando a mesma ideia anterior, trouxe os dados de valores movidos nos diferentes segmentos dos clientes. 

![valor por segmento](/imagens/valores_segmento.png)

Há, novamente, uma movimentação em Dezembro. Tanto indústria quanto tecnologia têm uma queda.

|![RL valor ](/imagens/rl_valores_varejo.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 73226.79511868008 |
|*[dados](/dados/rl_valor_varejo.csv)*|

|![RL valor ](/imagens/rl_valores_financeiro.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 48252.99739174837 |
|*[dados](/dados/rl_valores_financeiro.csv)*|

|![RL valor ](/imagens/rl_valores_industria.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 120117.22095728837 |
|*[dados](/dados/rl_valores_industria.csv)*|

|![RL valor ](/imagens/rl_valores_servicos.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 34324.97244659781 |
|*[dados](/dados/rl_valores_servicos.csv)*|

|![RL valor ](/imagens/rl_valores_tecnologia.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 132985.60717397023 |
|*[dados](/dados/rl_valores_tecnologia.csv)*|

![valor de faturamento por segmento](/imagens/faturamento_segmento.png)

|![RL valor ](/imagens/rl_faturamento_varejo.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 118328.62417770803 |
|*[dados](/dados/rl_faturamento_varejo.csv)*|

|![RL valor ](/imagens/rl_faturamento_financeiro.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 87628.33115304234 |
|*[dados](/dados/rl_faturamento_financeiro.csv)*|

|![RL valor ](/imagens/rl_faturamento_industria.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 169173.79974244014 |
|*[dados](/dados/rl_faturamento_industria.csv)*|

|![RL valor ](/imagens/rl_faturamento_tecnologia.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 199048.76365279293 |
|*[dados](/dados/rl_faturamento_tecnologia.csv)*|

|![RL valor ](/imagens/rl_faturamento_servicos.png)| 
|:--:| 
|  Root Mean Squared Error (RMSE): 59743.29320062869 |
|*[dados](/dados/rl_faturamento_servicos.csv)*|

Analisando os gráficos acima, os segmentos de **varejo** e **financeiro** são os que apresentam maior crescimento.  **Tecnologia** e **indústria** apresentam alguns outliers, não sei identificar o quanto isso influência na série.

# Outros modelos

Tentei aplicar os modelos ARMA/ARIMA/SARIMAX sem muitos resultados positivos. ACF/PACF mostraram que o melhor candidato para **p**,**q** é 0, indicando inexistência de autocorrelação nos dados. Por ser uma área que não domino, preferi abandonar a ideia. Abaixo alguns dos resultados obtidos.

- [ARMA](/imagens/ARMA.png)
- [ARIMA](/imagens/ARIMA.png)
- [SARIMAX](/imagens/SARIMAX.png)

![asd](/imagens/acf_pacf.png)


# Considerações finais

Quebra das séries por tipo de transação e segmento foi algo que me ocorreu no momento da escrita deste documento. Talvez alguns tipos de transação sejam mais padrão em alguns segmentos.

Está além do meu conhecimento, mas acredito que seja possível criar perfis de cliente cruzando as informações mostradas aqui. Por exemplo, cruzando o tipo de transação com o segmento do cliente.

Também é possível identificar eventos sazonais e qual a janela desses eventos conforme segmentos, talvez seja isso que foi observado nos meses de Novembro/Dezembro.

Por fim, embora minhas tentativas de usar modelos diferentes tenham sido falhas, existe uma lista enorme de possibilidades ali, sendo somente necessário identificar qual desses se encaixaria nesse caso.
