dataviva-scripts
================

Helper scripts for DataViva data calculations and transformations

* ```git clone https://github.com/DataViva/dataviva-scripts.git dataviva-scripts```
* ```cd dataviva-scripts```
* ```git submodule update --init --recursive```
* ```pip install -r requirements.txt ```

###Para utilizar o script do "load_metadata":

Primeiramente, deve-se acessar a máquina de ETL (etl_linux), para isso, deve-se:

* Ligar a máquina dentro da AWS
* Baixar a chave privada SSH
* Abrir um terminal linux local
* Rodar o comando com a chave para acesso à máquina

Agora, deve-se entrar na pasta com o arquivo “load_metadata”:

	* ```cd dev/scripts/dataviva-scripts```


Caso os requirements não estejam instalados na máquina, deve-se rodar o comando:

	* ```pip3 install -r requirements.txt```


Em caso da não instalação de algum requisito ou erro ao rodar o requirements, deve-se instalar manualmente cada requisito com o comando:

	* ```pip3 install “nome-requisito”```


Agora, para rodar o load_metadata, basta utilizar o comando:

	* ```python3 load_metadata.py “argumento”```

Os possíveis argumentos podem ser visualizados com o comando:

	* ```python3 load_metadata.py –help``` 


Caso o erro: "if VALID_BUCKET.search(bucket) is none" aconteça, para corrigi-lo, basta utilizar o comando: 
	
	* ```export S3_BUCKET=dataviva-etl```

### Caveats
* Need the following environment vars to be set:
 * ```DATAVIVA_DB_NAME```
 * ```DATAVIVA2_DB_NAME```
 * ```DATAVIVA_DB_USER```
 * ```DATAVIVA2_DB_USER```
 * ```DATAVIVA_DB_PW```
 * ```DATAVIVA2_DB_PW```
* Need unrar tool installed
 * ```brew install unrar```
