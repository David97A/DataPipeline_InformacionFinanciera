# DataPipeline_InformacionFinanciera
Diseño e Implementación de un Data Pipeline para extracción, manipulación, carga y presentación de Información Financiera.

## Descripcion General

El objetivo de este repositorio es el documentar el Diseño de un Flujo de Datos para extrer la información que publica la Comisión Nacional Bancaria y de Valores (CNBV) a través de su Sitio Web, transformar y estandarizar dicha información para su posterior carga en una Base de Datos Relacional PostgreSQL. Posteriormente, dicha información se desplegara en un Dashboard Interactivo con el objetivo de exponer de manera visual los KPI's y tendencias relevantes existentes detras de estos datos.

## Arquitectura de la Solucion

### Flujo de Datos

El Flujo para el tratamiento de la información inicia con la extracción de los archivos del Boletín Estadístico correspondientes a la Banca Múltiple hacia un repositorio de documentos local mediante Comandos Bash. Una vez depositado el archivo en el repositorio local, la información contenida en dichos documentos se limpia, estandariza y configura por medio de un Script en Python para ser depositada en un Modelo de Datos Dimensional de tipo PostgreSQL. Tanto el proceso de Extracción por Bash, como la Limpieza, Transformación y Carga a BD mediante Python son orquestados mediante un DAG de Apache Airflow.

Una vez depositada la Información en la BD Relacional, la misma es consumida por un Dashboard Interactivo desarrollado con la paquetería Shiny para el lenguaje de programación R. El objetivo final de la representación de la información por medio de un Dashboard es facilitar la lectura de KPI's y Tendencias identificadas en la Información publicada por la CNBV.


### Modelo de Datos Relacional

Para el Diseño del Modelo de Datos se seguirá la Metodología Dimensional de Kimball (Kimball & Ross, 2002), basada en arquitecturas Tipo Estrella y Copo de Nieve, con Objetos de Tipo "Fact Tables" al centro del Modelo y sus correspondientes "Dimensional Tables" al rededor para brindar el Contexto necesario a la información.

#### Modelo ER Inicial

Para el primer MVP, se iniciará con una Fact Table que almacene de manera histórica la información de Saldos de Crédito, así como el IMOR y el ICOR correspondientes a los Boletines Estadísticos publicados por la CNBV, identificados por Institución Financiera y Tipo de Cartera. Al rededor de este objeto, se consideran además dos "Dimensional Tables", las cuales, a manera de catálogos, complementarán el contexto de los Bancos y los Tipos de Cartera de Crédito almacenados en la Fact Table previamente explicada.

![alt_image](https://github.com/David97A/DataPipeline_InformacionFinanciera/blob/50b58c2474d94498c6a4b3f66267cd577753a47d/Recursos/Imagenes/ERD_ModeloInfoFinanciera_CNBV_01_20250622.png)
