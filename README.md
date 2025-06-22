# DataPipeline_InformacionFinanciera
Diseño e Implementación de un Data Pipeline para extracción, manipulación, carga y presentación de Información Financiera.

## Descripcion General

El objetivo de este repositorio es el documentar el Diseño de un Flujo de Datos para extrer la información que publica la Comisión Nacional Bancaria y de Valores (CNBV) a través de su Sitio Web, transformar y estandarizar dicha información para su posterior carga en una Base de Datos Relacional PostgreSQL. Posteriormente, dicha información se desplegara en un Dashboard Interactivo con el objetivo de exponer de manera visual los KPI's y tendencias relevantes existentes detras de estos datos.

## Arquitectura de la Solucion

### Modelo de Datos Relacional

Para el Diseño del Modelo de Datos se seguirá la Metodología Dimensional de Kimball (Kimball & Ross, 2002), basada en arquitecturas Tipo Estrella y Copo de Nieve, con Objetos de Tipo "Fact Tables" al centro del Modelo y sus correspondientes "Dimensional Tables" al rededor para brindar el Contexto necesario a la información.

#### Modelo ER Inicial

Para el primer MVP, se iniciará con una Fact Table que almacene de manera histórica la información de Saldos de Crédito, así como el IMOR y el ICOR correspondientes a los Boletines Estadísticos publicados por la CNBV, identificados por Institución Financiera y Tipo de Cartera. Al rededor de este objeto, se consideran además dos "Dimensional Tables", las cuales, a manera de catálogos, complementarán el contexto de los Bancos y los Tipos de Cartera de Crédito almacenados en la Fact Table previamente explicada.
