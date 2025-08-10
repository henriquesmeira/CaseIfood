-- ============================================================================
-- CASE TÉCNICO IFOOD - DATA ARCHITECT
-- Respostas às Perguntas de Negócio em SQL
-- ============================================================================

-- Configuração inicial
USE CATALOG main;
USE SCHEMA nyc_taxi;

-- ============================================================================
-- PERGUNTA 1: Qual a média de valor total (total_amount) recebido em um mês 
-- considerando todos os yellow taxis da frota?
-- ============================================================================

-- Resposta detalhada por mês
SELECT 
    'PERGUNTA 1 - DETALHAMENTO POR MÊS' as analise,
    year_month,
    ROUND(AVG(avg_total_amount), 2) as media_valor_total,
    SUM(total_trips) as total_viagens,
    ROUND(SUM(sum_total_amount), 2) as receita_total_mes
FROM main.nyc_taxi.gold_trips
WHERE taxi_type = 'yellow'
GROUP BY year_month
ORDER BY year_month;

-- Resposta principal - Média geral
SELECT 
    'PERGUNTA 1 - RESPOSTA PRINCIPAL' as analise,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as media_valor_total_yellow_taxis,
    SUM(total_trips) as total_viagens_yellow,
    ROUND(SUM(sum_total_amount), 2) as receita_total_yellow
FROM main.nyc_taxi.gold_trips
WHERE taxi_type = 'yellow';

-- Análise complementar - Comparação mensal
SELECT 
    'PERGUNTA 1 - ANÁLISE COMPLEMENTAR' as analise,
    year_month,
    ROUND(AVG(avg_total_amount), 2) as media_valor,
    SUM(total_trips) as viagens,
    ROUND(
        (SUM(total_trips) * 100.0) / 
        (SELECT SUM(total_trips) FROM main.nyc_taxi.gold_trips WHERE taxi_type = 'yellow'), 
        2
    ) as percentual_viagens_mes
FROM main.nyc_taxi.gold_trips
WHERE taxi_type = 'yellow'
GROUP BY year_month
ORDER BY year_month;

-- ============================================================================
-- PERGUNTA 2: Qual a média de passageiros (passenger_count) por cada hora do dia 
-- que pegaram táxi no mês de maio considerando todos os táxis da frota?
-- ============================================================================

-- Resposta principal - Média por hora em Maio
SELECT 
    'PERGUNTA 2 - RESPOSTA PRINCIPAL' as analise,
    pickup_hour as hora,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as media_passageiros_por_viagem,
    SUM(total_trips) as total_viagens,
    SUM(sum_passenger_count) as total_passageiros
FROM main.nyc_taxi.gold_trips
WHERE pickup_month = 5 
  AND pickup_year = 2023
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Análise complementar - Identificação de picos
SELECT 
    'PERGUNTA 2 - ANÁLISE DE PICOS' as analise,
    pickup_hour as hora,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as media_passageiros,
    SUM(total_trips) as viagens,
    CASE 
        WHEN pickup_hour BETWEEN 7 AND 9 THEN 'Pico Manhã'
        WHEN pickup_hour BETWEEN 17 AND 19 THEN 'Pico Tarde'
        WHEN pickup_hour BETWEEN 22 AND 23 THEN 'Pico Noite'
        ELSE 'Normal'
    END as periodo_demanda
FROM main.nyc_taxi.gold_trips
WHERE pickup_month = 5 
  AND pickup_year = 2023
GROUP BY pickup_hour
ORDER BY media_passageiros DESC;

-- Análise por período do dia em Maio
SELECT 
    'PERGUNTA 2 - POR PERÍODO DO DIA' as analise,
    pickup_period as periodo,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as media_passageiros,
    SUM(total_trips) as total_viagens,
    ROUND(
        (SUM(total_trips) * 100.0) / 
        (SELECT SUM(total_trips) FROM main.nyc_taxi.gold_trips 
         WHERE pickup_month = 5 AND pickup_year = 2023), 
        2
    ) as percentual_viagens
FROM main.nyc_taxi.gold_trips
WHERE pickup_month = 5 
  AND pickup_year = 2023
GROUP BY pickup_period
ORDER BY media_passageiros DESC;

-- ============================================================================
-- ANÁLISES COMPLEMENTARES E INSIGHTS
-- ============================================================================

-- Comparação Yellow vs Green Taxis
SELECT 
    'ANÁLISE COMPLEMENTAR - YELLOW vs GREEN' as analise,
    taxi_type,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as media_tarifa,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as media_passageiros,
    SUM(total_trips) as total_viagens,
    ROUND(SUM(sum_total_amount), 2) as receita_total
FROM main.nyc_taxi.gold_trips
GROUP BY taxi_type
ORDER BY total_viagens DESC;

-- Evolução mensal geral
SELECT 
    'ANÁLISE COMPLEMENTAR - EVOLUÇÃO MENSAL' as analise,
    year_month,
    SUM(total_trips) as viagens_mes,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as tarifa_media_mes,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as passageiros_medio_mes,
    ROUND(SUM(sum_total_amount), 2) as receita_mes
FROM main.nyc_taxi.gold_trips
GROUP BY year_month
ORDER BY year_month;

-- Top 5 horas com mais viagens
SELECT 
    'ANÁLISE COMPLEMENTAR - TOP HORAS' as analise,
    pickup_hour as hora,
    SUM(total_trips) as total_viagens,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as tarifa_media,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as passageiros_medio
FROM main.nyc_taxi.gold_trips
GROUP BY pickup_hour
ORDER BY total_viagens DESC
LIMIT 5;

-- Análise de fins de semana vs dias úteis
SELECT 
    'ANÁLISE COMPLEMENTAR - FINS DE SEMANA' as analise,
    CASE WHEN is_weekend THEN 'Fim de Semana' ELSE 'Dia Útil' END as tipo_dia,
    SUM(total_trips) as viagens,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as tarifa_media,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as passageiros_medio,
    ROUND(
        (SUM(total_trips) * 100.0) / 
        (SELECT SUM(total_trips) FROM main.nyc_taxi.gold_trips), 
        2
    ) as percentual_viagens
FROM main.nyc_taxi.gold_trips
GROUP BY is_weekend
ORDER BY viagens DESC;

-- ============================================================================
-- VALIDAÇÃO DOS DADOS
-- ============================================================================

-- Verificação de qualidade dos dados
SELECT 
    'VALIDAÇÃO - QUALIDADE DOS DADOS' as analise,
    COUNT(*) as total_registros_gold,
    COUNT(DISTINCT taxi_type) as tipos_taxi,
    COUNT(DISTINCT year_month) as meses_disponiveis,
    MIN(pickup_year) as ano_minimo,
    MAX(pickup_year) as ano_maximo,
    SUM(total_trips) as total_viagens_agregadas
FROM main.nyc_taxi.gold_trips;

-- Verificação de cobertura temporal
SELECT 
    'VALIDAÇÃO - COBERTURA TEMPORAL' as analise,
    taxi_type,
    year_month,
    SUM(total_trips) as viagens,
    COUNT(DISTINCT pickup_hour) as horas_cobertas
FROM main.nyc_taxi.gold_trips
GROUP BY taxi_type, year_month
ORDER BY taxi_type, year_month;

-- ============================================================================
-- RESUMO EXECUTIVO
-- ============================================================================

-- Métricas principais para apresentação
SELECT 
    'RESUMO EXECUTIVO' as analise,
    'Métrica' as indicador,
    'Valor' as resultado
UNION ALL
SELECT 
    'RESUMO EXECUTIVO',
    'Total de Viagens',
    FORMAT_NUMBER(SUM(total_trips), 0)
FROM main.nyc_taxi.gold_trips
UNION ALL
SELECT 
    'RESUMO EXECUTIVO',
    'Receita Total',
    CONCAT('$', FORMAT_NUMBER(SUM(sum_total_amount), 2))
FROM main.nyc_taxi.gold_trips
UNION ALL
SELECT 
    'RESUMO EXECUTIVO',
    'Média Yellow Taxis',
    CONCAT('$', FORMAT_NUMBER(
        SUM(CASE WHEN taxi_type = 'yellow' THEN sum_total_amount ELSE 0 END) / 
        SUM(CASE WHEN taxi_type = 'yellow' THEN total_trips ELSE 0 END), 2
    ))
FROM main.nyc_taxi.gold_trips
UNION ALL
SELECT 
    'RESUMO EXECUTIVO',
    'Média Passageiros Maio',
    FORMAT_NUMBER(
        SUM(CASE WHEN pickup_month = 5 THEN sum_passenger_count ELSE 0 END) / 
        SUM(CASE WHEN pickup_month = 5 THEN total_trips ELSE 0 END), 2
    )
FROM main.nyc_taxi.gold_trips;

-- ============================================================================
-- FIM DAS CONSULTAS
-- ============================================================================
