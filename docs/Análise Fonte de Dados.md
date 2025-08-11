# 📁 Análise da Fonte de Dados - E-commerce API

A API está disponível em diferentes endpoints na URL <https://api.gustavonucci.dev/ecomm/v1>.

Os endpoints da API estão disponíveis no [swagger](https://api.gustavonucci.dev/docs).

## 1. Arquitetura dos Dados

A arquitetura de dados da API de e-commerce foi estruturada seguindo princípios de modelagem relacional, organizando as informações em entidades bem definidas que representam os principais componentes de um sistema de comércio eletrônico.

Esta seção apresenta uma análise detalhada das entidades, seus relacionamentos e a estrutura que servirá como base para a ingestão de dados no data warehouse, facilitando a compreensão do modelo de negócio e permitindo o planejamento adequado das transformações necessárias nas camadas Bronze, Silver e Gold da arquitetura Medallion.

### 1.1 Entidades Principais

| Entidade         | Relacionamentos                          | Campos Principais          |
| ---------------- | ---------------------------------------- | -------------------------- |
| **Clientes**     | 1:N com Endereços, 1:N com Vendas        | ID, Nome, Email, CPF       |
| **Produtos**     | N:1 com Categorias, N:1 com Fornecedores | ID, Nome, Preço, Estoque   |
| **Categorias**   | 1:N com Produtos                         | ID, Nome, Descrição        |
| **Fornecedores** | 1:N com Produtos                         | ID, Nome, CNPJ             |
| **Vendas**       | N:1 com Clientes, 1:N com Itens          | ID, Data, Total            |
| **Endereços**    | N:1 com Clientes                         | ID, Logradouro, CEP        |
| **Itens Venda**  | N:1 com Vendas, N:1 com Produtos         | Quantidade, Preço Unitário |

### 1.2 Relacionamentos do Sistema

```text
Cliente (1) ←→ (N) Endereço
Cliente (1) ←→ (N) Venda
Venda (1) ←→ (N) Item_Venda
Produto (1) ←→ (N) Item_Venda
Categoria (1) ←→ (N) Produto
Fornecedor (1) ←→ (N) Produto
```

## 2. APIs Disponíveis e Formatos

Esta seção detalha os endpoints disponíveis na API de e-commerce, fornecendo uma visão abrangente das funcionalidades oferecidas e dos formatos de dados utilizados.

A documentação dos endpoints é essencial para o planejamento da ingestão de dados, pois permite identificar as fontes de informação disponíveis, compreender a estrutura dos dados retornados e definir as estratégias de coleta que serão implementadas nos DAGs do Airflow.

Cada endpoint representa uma fonte de dados específica que alimentará as diferentes camadas do data warehouse.

### 2.1 Endpoints Identificados

| Módulo           | Funcionalidades Estimadas         |
| ---------------- | --------------------------------- |
| **Categorias**   | CRUD de categorias de produtos    |
| **Clientes**     | Gestão de clientes e autenticação |
| **Endereços**    | Gestão de endereços de entrega    |
| **Fornecedores** | CRUD de fornecedores              |
| **Produtos**     | Catálogo e gestão de produtos     |
| **Vendas**       | Processamento de pedidos          |
| **Health**       | Monitoramento da API              |

### 2.2 Formato de Dados

- **Entrada/Saída**: JSON (padrão FastAPI)

## Database Schema

![Database Schema](https://github.com/user-attachments/assets/92533b96-e8c2-4fb4-a735-d598061e8063)
