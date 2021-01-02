# Indice Reverso - Textos do projeto Gutenberg

## Objetivos
* Ler todos os arquivos de textos e criar um dicionario no formato:  
  Formato:  
  (palavra, idPalavra)
    
* Criar um indice reverso aonde a key seja o id da Palavra e o value seja uma 
  lista dos documentos aonde existe ocorrencia da Palavra.  
  Formato:
  (palavraId, [docId1, docId2, docIdN])
  
## Ambiente utilizado
Para a solucao deste problema foi utilizada a Hortonworks SandBox 2.6.5.  
Esta Sandbox e uma Oracle Virtual Box que oferece um cluster com apenas uma 
maquina, que serve para o desenvolvimento e testes, para posterior deploy num 
cluster aonde ocorrera o processamento em paralelo.  
(https://www.cloudera.com/downloads/hortonworks-sandbox/hdp.html)  

### Versoes
* Python 2.7.5  
* HDFS 2.7.3  
* Spark 2.3.0  
* HDP 2.6.5
  
## Steps
1. Clonar projeto do Git
2. Carregar arquivos no HDFS  
3. Gerar dicionario de palavras
4. Criar Indice Reverso

### 1. Git Repository clone
* 1.1. Conectar no cluster Hadoop
```shell
ssh -p 2222 maria_dev@localhost
# password: maria_dev
```
* 1.2. Clonar o repositorio  
Para este guia, será usada a pasta home, mas pode ser clonado em qualquer pasta
  de preferência.
```shell
git clone https://github.com/alexcarvalhodata/gutenbergReverseIndex.git
```

### 2. Carregando os arquivos no HDFS
2.1. Criar pastar no HDFS para armazenar o dataset  
```shell
hadoop fs -mkdir -p project22/output
```
2.2. Carregar os arquivos no HDFS do cluster
```shell
hadoop fs -copyFromLocal ~/gutenbergReverseIndex/dataset project22/dataset
```
### 3. Gerando dicionario de palavras
3.1. Ir para a pasta onde foi clonado o repositório do git
```shell
cd ~/gutenbergReverseIndex
```
3.2. Executar o job que cria o Dicionário
```shell
spark-submit dictionary_builder.py
```
### 4. Criando Indice Reverso
4.1. Executar o job que cria o Indice
```shell
spark-submit reverse_index_builder.py
```
### 5. Recuperando arquivos gerados no HDFS para o File System
5.1. Criando diretório de saída
```shell
mkdir ~/gutenbergReverseIndex/output
```
5.2. Recuperando dictionário
```shell
hadoop fs -copyToLocal project22/output/words_dictionary/part*.csv ~/gutenbergReverseIndex/output/words_dictionary.txt
```
5.3. Recuperando índice reverso
```shell
hadoop fs -copyToLocal project22/output/word_reverse_idx/part*.csv ~/gutenbergReverseIndex/output/word_reverse_idx.txt
```
5.4. Arquivos disponibilizados em:
```shell
cd ~/gutenbergReverseIndex/output
```
