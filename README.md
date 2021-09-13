# README
## Sobre este repositório
Este repositório contém todo o material utilizado para a confecção do trabalho final do curso de estatística de Daniel dos Santos na Universidade Federal Fluminense (UFF) sobre a utilização do Spark utilizando a linguagem de programação R em conjunto da API SparkR.

### Resumo
Desde o começo da Terceira Revolução Industrial, o volume de dados armazenados cresce exponencialmente, marcando este período como a Era da Informação. A capacidade de explorar tamanha quantidade de dados abre oportunidades para novas formas de análise e descobertas. Com o intuito de realizar tais análises de larga escala foi desenvolvido o Apache Spark, um framework de código aberto que busca democratizar estudos com dados de alta dimensão, utilizando técnicas de computação distribuída já fornecidas  pelo MapReduce, porém com grandes melhorias em performance e praticidade.
O Spark possui uma série de componentes que envolvem aprendizado de máquinas, análise de grafos, processamento de dados em tempo real e a realização de análises estatísticas em grandes volumes de dados.
O intuito deste trabalho é explicar, apresentar e explorar a gama de ferramentas encontradas no Spark, utilizando-se das tecnologias e arquiteturas encontradas nele em conjunto com a linguagem de programação R a partir da biblioteca SparkR.

Palavras-chave: Apache Spark. Big data. Computação distribuída. Engenharia de dados. MapReduce. R.

## Como utilizar este repositório?
A organização das pastas e arquivos:
- /paper: Pasta que contém a versão final do trabalho defendindo;
- /references: Pasta que contém as referências de acesso público e de livre disponibilização;
- /source: Pasta que contém arquivos de imagens, bancos de dados e scripts utilizados na monografia;
- /source/images: Pasta que contém as imagens utilizadas na dissertação. Foram criados subdiretórios que correspondem as seções e subseções do texto, da seguinte forma: "/source/image/02/01" corresponde as imagens da seção 2 e subseção 1 do material;
- /source/scripts: Pasta que contém os scripts. Obedencem a mesma regra das imagens;

A seguir a árvore de arquivos:
```
.
├── README.md
├── paper
├── references
│   ├── 77384.pdf
│   ├── README.md
│   ├── abdul_ghaffar.pdf
│   ├── christian_pentzold.pdf
│   ├── jeffrey_dean.pdf
│   ├── matei_zaharia.pdf
│   ├── perry_patrick.pdf
│   ├── ripon_patgiri.pdf
│   ├── salman_salloum.pdf
│   ├── spolador_rodolfo.pdf
│   ├── wagner_kolberg.pdf
│   └── xiangrui_meng.pdf
└── source
    ├── data
    │   ├── particles.csv
    │   └── used_cars_data.csv
    ├── images
    │   └── 02
    │       ├── 02
    │       │   └── 01_spark-logo-trademark.png
    │       ├── 03
    │       │   ├── 01_mapreduce-example.png
    │       │   └── 02_mapreduce-parallel-example.png
    │       ├── 04
    │       │   └── 01_mapreduce-vs-spark.png
    │       ├── 05
    │       │   ├── 01_fault-tolerance.png
    │       │   └── 02_fault-tolerance-stopped-working.png
    │       ├── 06
    │       │   ├── 01_spark-ecosystem.png
    │       │   └── 03
    │       │       └── 01_spark-streaming-example.png
    │       ├── 07
    │       │   └── 02
    │       │       └── 01_spark-and-r.png
    │       ├── 08
    │       │   ├── 02
    │       │   │   └── 01_spark-download-page.png
    │       │   ├── 03
    │       │   │   └── 01_spark-interface-page.png
    │       │   └── 04
    │       │       └── 01_spark-collect-base.png
    │       └── 10
    │           └── 02
    │               ├── 01_example-barplot.png
    │               ├── 02_example-histogram.png
    │               ├── 03_example-boxplot.png
    │               ├── 04_example-point.png
    │               └── 05_example-bivariate-histogram.png
    └── scripts
        ├── 02
        │   ├── 08
        │   │   ├── 03
        │   │   │   └── starting_spark_session.R
        │   │   └── 04
        │   │       └── first_steps.R
        │   ├── 09
        │   │   ├── 01
        │   │   │   └── basic_operations.R
        │   │   ├── 02
        │   │   │   └── selection.R
        │   │   ├── 03
        │   │   │   └── filtering.R
        │   │   ├── 04
        │   │   │   └── creating_new_columns.R
        │   │   └── 05
        │   │       └── using_sql_in_sparkr.R
        │   ├── 10
        │   │   ├── 01
        │   │   │   └── descriptive_statistics.R
        │   │   ├── 02
        │   │   │   └── data_visualization.R
        │   │   └── exploratory_analysis.R
        │   └── 11
        │       ├── 01
        │       │   └── data_preparation.R
        │       ├── 02
        │       │   └── model_trainning.R
        │       ├── 03
        │       │   └── model_evaluation.R
        │       └── 04
        │           ├── model
        │           └── saving_loading_models.R
        └── 03
            └── 01
                ├── graphs
                │   ├── bar
                │   ├── boxplot
                │   ├── histograms
                │   └── pareto
                ├── meta
                │   ├── metadata.txt
                │   └── metadata_pt.txt
                ├── model
                │   ├── decisionTree_cars
                │   └── decisionTree_cars_01
                └── used_cars.R

51 directories, 49 files
```

Qualquer dúvida ou sugestão serão bem vindas. 
Muito obrigado pelo interesse neste trabalho. 
