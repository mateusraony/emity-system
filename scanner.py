"""
Motor de Análise com IA - EMITY System
Analisa pools e gera ranges otimizados
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import math
import json

logger = logging.getLogger(__name__)

class PoolAnalyzer:
    """Analisador de pools com IA"""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        
    async def analyze_all_pools(self):
        """Analisa todas as pools do banco"""
        try:
            # Buscar pools do banco
            response = self.supabase.table('pools').select('address').execute()
            
            if not response.data:
                logger.info("Nenhuma pool para analisar")
                return []
            
            analyzed_count = 0
            for pool in response.data:
                try:
                    await self.analyze_pool(pool['address'])
                    analyzed_count += 1
                except Exception as e:
                    logger.error(f"Erro ao analisar pool {pool['address']}: {str(e)}")
                    continue
            
            logger.info(f"✅ {analyzed_count} pools analisadas com sucesso")
            return analyzed_count
            
        except Exception as e:
            logger.error(f"Erro ao buscar pools: {str(e)}")
            return 0
    
    async def analyze_pool(self, pool_address: str) -> Dict:
        """Analisa uma pool específica"""
        try:
            # Buscar dados da pool
            response = self.supabase.table('pools').select('*').eq('address', pool_address).execute()
            
            if not response.data:
                logger.error(f"Pool {pool_address} não encontrada")
                return None
            
            pool = response.data[0]
            
            # Validar e corrigir valores None
            pool = self._validate_pool_data(pool)
            
            # Gerar ranges otimizados
            ranges = self._generate_ranges(pool)
            
            # Simular retornos
            simulations = self._simulate_returns(pool, ranges)
            
            # Preparar dados para salvar (APENAS campos que existem na tabela)
            analysis_data = {
                'ranges_data': json.dumps(ranges),
                'simulations_data': json.dumps(simulations),
                'last_analyzed': datetime.now().isoformat()
            }
            
            # Atualizar pool no banco
            self.supabase.table('pools').update(analysis_data).eq('address', pool_address).execute()
            
            logger.info(f"✅ Análise salva para pool {pool_address}")
            
            return {
                'address': pool_address,
                'ranges': ranges,
                'simulations': simulations
            }
            
        except Exception as e:
            logger.error(f"Erro ao analisar pool {pool_address}: {str(e)}")
            return None
    
    def _validate_pool_data(self, pool: Dict) -> Dict:
        """Valida e corrige dados da pool com valores None"""
        # Garantir que todos os campos numéricos tenham valores válidos
        pool['tvl_usd'] = float(pool.get('tvl_usd') or 0)
        pool['volume_24h'] = float(pool.get('volume_24h') or 0)
        pool['fees_24h'] = float(pool.get('fees_24h') or 0)
        pool['current_price'] = float(pool.get('current_price') or 0)
        pool['price_change_24h'] = float(pool.get('price_change_24h') or 0)
        pool['fee_tier'] = float(pool.get('fee_tier') or 0.3)
        pool['score'] = int(pool.get('score') or 0)
        
        # Garantir strings
        pool['token0_symbol'] = pool.get('token0_symbol') or 'UNKNOWN'
        pool['token1_symbol'] = pool.get('token1_symbol') or 'UNKNOWN'
        
        return pool
    
    def _generate_ranges(self, pool: Dict) -> Dict:
        """Gera 3 ranges otimizados"""
        current_price = pool.get('current_price', 0)
        
        # Se não tem preço, usar valores default
        if current_price <= 0:
            current_price = 1000  # Valor default
        
        volatility = abs(pool.get('price_change_24h', 2.5))
        
        # Ajustar volatilidade se for 0
        if volatility == 0:
            volatility = 2.5  # 2.5% default
        
        # Range defensivo (conservador)
        defensive_range = {
            'min_price': current_price * (1 - volatility * 3 / 100),
            'max_price': current_price * (1 + volatility * 3 / 100),
            'spread_percent': volatility * 3,
            'strategy': 'defensive',
            'description': '🛡️ Conservador - Range amplo para menor risco'
        }
        
        # Range otimizado (balanceado)
        optimized_range = {
            'min_price': current_price * (1 - volatility * 1.5 / 100),
            'max_price': current_price * (1 + volatility * 1.5 / 100),
            'spread_percent': volatility * 1.5,
            'strategy': 'optimized',
            'description': '⚖️ Balanceado - Equilíbrio entre risco e retorno'
        }
        
        # Range agressivo (maior retorno)
        aggressive_range = {
            'min_price': current_price * (1 - volatility * 0.5 / 100),
            'max_price': current_price * (1 + volatility * 0.5 / 100),
            'spread_percent': volatility * 0.5,
            'strategy': 'aggressive',
            'description': '🚀 Agressivo - Range estreito para máximo retorno'
        }
        
        return {
            'defensive': defensive_range,
            'optimized': optimized_range,
            'aggressive': aggressive_range
        }
    
    def _simulate_returns(self, pool: Dict, ranges: Dict) -> Dict:
        """Simula retornos para 7 e 30 dias"""
        # Calcular APR base
        fees_24h = pool.get('fees_24h', 0)
        tvl = pool.get('tvl_usd', 1)  # Evitar divisão por 0
        
        if tvl <= 0:
            tvl = 1  # Valor mínimo
        
        apr_base = (fees_24h * 365 / tvl * 100) if tvl > 0 else 0
        
        simulations = {}
        
        for strategy_name, range_data in ranges.items():
            # Fator de concentração baseado no spread
            spread = range_data['spread_percent']
            if spread > 0:
                concentration_factor = 10 / spread  # Quanto menor o spread, maior a concentração
            else:
                concentration_factor = 1
                
            concentration_factor = min(3, max(0.5, concentration_factor))  # Limitar entre 0.5 e 3
            
            # Estimar tempo em range baseado na volatilidade
            volatility = abs(pool.get('price_change_24h', 2.5))
            if strategy_name == 'defensive':
                time_in_range_7d = min(95, 90 + (5 - volatility))
                time_in_range_30d = min(95, 85 + (5 - volatility))
            elif strategy_name == 'optimized':
                time_in_range_7d = min(85, 75 + (5 - volatility))
                time_in_range_30d = min(80, 65 + (5 - volatility))
            else:  # aggressive
                time_in_range_7d = min(70, 50 + (5 - volatility))
                time_in_range_30d = min(60, 40 + (5 - volatility))
            
            # Calcular retornos
            apr_adjusted = apr_base * concentration_factor
            
            # Simulação 7 dias
            fees_7d = (apr_adjusted * 7 / 365) * (time_in_range_7d / 100)
            il_7d = volatility * 0.1 * (1 / concentration_factor)
            
            # Simulação 30 dias
            fees_30d = (apr_adjusted * 30 / 365) * (time_in_range_30d / 100)
            il_30d = volatility * 0.3 * (1 / concentration_factor)
            
            simulations[strategy_name] = {
                '7d': {
                    'time_in_range': round(time_in_range_7d, 1),
                    'fees_collected': round(fees_7d, 2),
                    'impermanent_loss': round(il_7d, 2),
                    'net_return': round(fees_7d - il_7d, 2),
                    'gas_cost': 5,
                    'net_after_gas': round(fees_7d - il_7d - 5, 2)
                },
                '30d': {
                    'time_in_range': round(time_in_range_30d, 1),
                    'fees_collected': round(fees_30d, 2),
                    'impermanent_loss': round(il_30d, 2),
                    'net_return': round(fees_30d - il_30d, 2),
                    'gas_cost': 5,
                    'net_after_gas': round(fees_30d - il_30d - 5, 2)
                }
            }
        
        return simulations

async def run_analyzer(supabase_client):
    """Função auxiliar para executar o analisador"""
    analyzer = PoolAnalyzer(supabase_client)
    return await analyzer.analyze_all_pools()
