"""
Motor de Análise com IA - EMITY System
Gera ranges otimizados e calcula scores institucionais
"""

import math
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def to_float(value, default: float = 0.0) -> float:
    """
    Converte valores variados para float de forma segura.
    Evita erros quando o valor é None, string vazia ou inválido.
    """
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

class PoolAnalyzer:
    """Analisador avançado de pools com geração de ranges"""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        
        # Parâmetros de range
        self.RANGE_MULTIPLIERS = {
            'defensive': {'lower': 0.85, 'upper': 1.15},  # ±15%
            'optimized': {'lower': 0.92, 'upper': 1.08},  # ±8%
            'aggressive': {'lower': 0.96, 'upper': 1.04}   # ±4%
        }
        
        # Pesos para score
        self.SCORE_WEIGHTS = {
            'tvl': 0.20,
            'volume': 0.15,
            'fee_apr': 0.25,
            'il_risk': 0.20,
            'volatility': 0.10,
            'time_in_range': 0.10
        }
    
    async def analyze_pool(self, pool_address: str) -> Dict:
        """Análise completa de uma pool"""
        try:
            # Buscar dados da pool
            result = self.supabase.table('pools').select('*').eq('address', pool_address).execute()
            
            if not result.data:
                logger.error(f"Pool {pool_address} não encontrada")
                return None
            
            pool = result.data[0]
            
            # Gerar ranges otimizados
            ranges = self._generate_ranges(pool)
            
            # Simular retornos para cada range
            simulations = self._simulate_returns(pool, ranges)
            
            # Calcular score institucional refinado
            score, explanation = self._calculate_institutional_score(pool, simulations)
            
            # Montar resultado completo
            analysis = {
                'pool_address': pool_address,
                'ranges': ranges,
                'simulations': simulations,
                'score': score,
                'explanation': explanation,
                'recommendation': self._generate_recommendation(score, simulations),
                'analyzed_at': datetime.utcnow().isoformat()
            }
            
            # Salvar análise
            await self._save_analysis(pool_address, analysis)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Erro ao analisar pool {pool_address}: {str(e)}")
            return None
    
    def _generate_ranges(self, pool: Dict) -> Dict:
        """Gera 3 ranges otimizados baseados no preço atual"""
        current_price = to_float(pool.get('current_price'), 1.0)
        volatility = to_float(pool.get('volatility'), 10.0)
        
        # Ajustar ranges baseado na volatilidade
        volatility_factor = 1 + (volatility / 100)
        
        ranges = {}
        for strategy, multipliers in self.RANGE_MULTIPLIERS.items():
            # Calcular limites ajustados
            lower_mult = multipliers['lower']
            upper_mult = multipliers['upper']
            
            # Expandir range em pools voláteis
            if volatility > 15:
                lower_mult *= (1 - (volatility - 15) / 100)
                upper_mult *= (1 + (volatility - 15) / 100)
            
            ranges[strategy] = {
                'min_price': current_price * lower_mult,
                'max_price': current_price * upper_mult,
                'spread_percent': (upper_mult - lower_mult) * 100,
                'strategy': strategy,
                'description': self._get_strategy_description(strategy)
            }
        
        return ranges
    
    def _simulate_returns(self, pool: Dict, ranges: Dict) -> Dict:
        """Simula retornos para cada range em 7 e 30 dias"""
        simulations = {}
        
        for strategy, range_data in ranges.items():
            sim = {
                '7d': self._calculate_period_returns(pool, range_data, 7),
                '30d': self._calculate_period_returns(pool, range_data, 30)
            }
            simulations[strategy] = sim
        
        return simulations
    
    def _calculate_period_returns(self, pool: Dict, range_data: Dict, days: int) -> Dict:
        """Calcula retornos para um período específico"""
        # Dados base
        fee_tier = to_float(pool.get('fee_tier'), 0.3)
        daily_volume = to_float(pool.get('volume_24h'), 0.0)
        tvl = to_float(pool.get('tvl_usd'), 1.0)
        volatility = to_float(pool.get('volatility'), 10.0)
        
        # Estimar tempo em range baseado na volatilidade e spread
        spread = range_data['spread_percent']
        time_in_range_prob = self._estimate_time_in_range(volatility, spread)
        
        # Calcular fees coletadas
        if tvl <= 0:
            tvl = 1.0  # proteção extra contra divisão por zero
        daily_fee_rate = (daily_volume * fee_tier / 100) / tvl
        total_fees = daily_fee_rate * days * time_in_range_prob * 100  # em %
        
        # Calcular IL estimada
        il_7d_val = to_float(pool.get('il_7d'), 0.0)
        il_daily = il_7d_val / 7
        total_il = il_daily * days * time_in_range_prob
        
        # Retorno líquido
        net_return = total_fees - total_il
        
        # Gas estimado (Arbitrum)
        gas_cost_usd = 5  # ~$5 por operação no Arbitrum
        
        return {
            'time_in_range': round(time_in_range_prob * 100, 1),
            'fees_collected': round(total_fees, 2),
            'impermanent_loss': round(total_il, 2),
            'net_return': round(net_return, 2),
            'gas_cost': gas_cost_usd,
            'net_after_gas': round(net_return - (gas_cost_usd / 1000 * 100), 2)  # Assumindo $1000 de capital
        }
    
    def _estimate_time_in_range(self, volatility: float, spread: float) -> float:
        """Estima probabilidade de ficar em range"""
        # Fórmula simplificada baseada em volatilidade vs spread
        if spread <= 0:
            return 0.0
        
        # Quanto maior o spread em relação à volatilidade, mais tempo em range
        ratio = spread / max(volatility, 1.0)
        
        if ratio > 3:
            return 0.95
        elif ratio > 2:
            return 0.85
        elif ratio > 1.5:
            return 0.75
        elif ratio > 1:
            return 0.65
        elif ratio > 0.5:
            return 0.50
        else:
            return 0.35
    
    def _calculate_institutional_score(self, pool: Dict, simulations: Dict) -> Tuple[int, str]:
        """Calcula score institucional refinado com explicação"""
        scores = {}
        
        # Score de TVL (0-100)
        tvl = to_float(pool.get('tvl_usd'), 0.0)
        if tvl > 5000000:
            scores['tvl'] = 100
        elif tvl > 1000000:
            scores['tvl'] = 80
        elif tvl > 500000:
            scores['tvl'] = 60
        elif tvl > 100000:
            scores['tvl'] = 40
        else:
            scores['tvl'] = 20
        
        # Score de Volume (0-100)
        volume = to_float(pool.get('volume_24h'), 0.0)
        if volume > 1000000:
            scores['volume'] = 100
        elif volume > 500000:
            scores['volume'] = 80
        elif volume > 100000:
            scores['volume'] = 60
        elif volume > 50000:
            scores['volume'] = 40
        else:
            scores['volume'] = 20
        
        # Score de Fee APR (0-100)
        fee_apr = to_float(pool.get('fee_apr'), 0.0)
        if fee_apr > 100:
            scores['fee_apr'] = 100
        elif fee_apr > 50:
            scores['fee_apr'] = 80
        elif fee_apr > 25:
            scores['fee_apr'] = 60
        elif fee_apr > 10:
            scores['fee_apr'] = 40
        else:
            scores['fee_apr'] = 20
        
        # Score de Risco IL (invertido, 0-100)
        il_7d = to_float(pool.get('il_7d'), 0.0)
        if il_7d < 1:
            scores['il_risk'] = 100
        elif il_7d < 2:
            scores['il_risk'] = 80
        elif il_7d < 5:
            scores['il_risk'] = 60
        elif il_7d < 10:
            scores['il_risk'] = 40
        else:
            scores['il_risk'] = 20
        
        # Score de Volatilidade (invertido, 0-100)
        volatility = to_float(pool.get('volatility'), 0.0)
        if volatility < 5:
            scores['volatility'] = 100
        elif volatility < 10:
            scores['volatility'] = 80
        elif volatility < 15:
            scores['volatility'] = 60
        elif volatility < 25:
            scores['volatility'] = 40
        else:
            scores['volatility'] = 20
        
        # Score de Time in Range (melhor simulação)
        best_time_in_range = max(
            simulations['defensive']['30d']['time_in_range'],
            simulations['optimized']['30d']['time_in_range'],
            simulations['aggressive']['30d']['time_in_range']
        )
        scores['time_in_range'] = min(100, best_time_in_range * 1.2)
        
        # Calcular score final ponderado
        final_score = 0.0
        for metric, weight in self.SCORE_WEIGHTS.items():
            final_score += scores.get(metric, 0) * weight
        
        final_score_int = int(final_score)
        
        # Gerar explicação
        explanation = self._generate_score_explanation(scores, final_score_int, pool)
        
        return final_score_int, explanation
    
    def _generate_score_explanation(self, scores: Dict, final_score: int, pool: Dict) -> str:
        """Gera explicação em português do score"""
        token0 = pool.get('token0_symbol', 'TOKEN0')
        token1 = pool.get('token1_symbol', 'TOKEN1')
        
        if final_score >= 80:
            quality = "EXCELENTE"
            emoji = "🌟"
        elif final_score >= 60:
            quality = "BOA"
            emoji = "✅"
        elif final_score >= 40:
            quality = "MODERADA"
            emoji = "⚠️"
        else:
            quality = "BAIXA"
            emoji = "❌"
        
        explanation = f"{emoji} Pool {token0}/{token1} - Qualidade {quality}\n\n"
        explanation += f"📊 Score Institucional: {final_score}/100\n\n"
        
        # Valores numéricos seguros para exibição
        tvl_value = to_float(pool.get('tvl_usd'), 0.0)
        fee_apr_value = to_float(pool.get('fee_apr'), 0.0)
        volume_value = to_float(pool.get('volume_24h'), 0.0)
        volatility_value = to_float(pool.get('volatility'), 0.0)
        il_7d_value = to_float(pool.get('il_7d'), 0.0)
        
        # Pontos fortes
        strengths = []
        if scores.get('tvl', 0) >= 60:
            strengths.append(f"✅ TVL sólida: ${tvl_value:,.0f}")
        if scores.get('fee_apr', 0) >= 60:
            strengths.append(f"✅ APR atrativa: {fee_apr_value:.1f}%")
        if scores.get('volume', 0) >= 60:
            strengths.append(f"✅ Volume alto: ${volume_value:,.0f}/24h")
        
        if strengths:
            explanation += "Pontos Fortes:\n" + "\n".join(strengths) + "\n\n"
        
        # Pontos de atenção
        weaknesses = []
        if scores.get('volatility', 0) < 60:
            weaknesses.append(f"⚠️ Volatilidade: {volatility_value:.1f}%")
        if scores.get('il_risk', 0) < 60:
            weaknesses.append(f"⚠️ Risco IL: {il_7d_value:.1f}% (7d)")
        
        if weaknesses:
            explanation += "Pontos de Atenção:\n" + "\n".join(weaknesses)
        
        return explanation
    
    def _generate_recommendation(self, score: int, simulations: Dict) -> str:
        """Gera recomendação baseada na análise"""
        # Encontrar melhor estratégia
        best_strategy = None
        best_return = -999.0
        
        for strategy, sim in simulations.items():
            net_30d = sim['30d']['net_after_gas']
            if net_30d > best_return:
                best_return = net_30d
                best_strategy = strategy
        
        if score >= 70 and best_return > 10:
            return f"💎 FORTE COMPRA - Range {best_strategy} com retorno estimado de {best_return:.1f}% em 30d"
        elif score >= 50 and best_return > 5:
            return f"✅ COMPRA - Range {best_strategy} com retorno estimado de {best_return:.1f}% em 30d"
        elif score >= 30 and best_return > 0:
            return f"⚠️ NEUTRO - Avaliar risco/retorno. Range {best_strategy}: {best_return:.1f}% em 30d"
        else:
            return f"❌ EVITAR - Score baixo ou retorno negativo"
    
    def _get_strategy_description(self, strategy: str) -> str:
        """Retorna descrição da estratégia"""
        descriptions = {
            'defensive': '🛡️ Conservador - Range amplo para menor risco',
            'optimized': '⚖️ Balanceado - Equilibrio entre risco e retorno',
            'aggressive': '🚀 Agressivo - Range estreito para máximo retorno'
        }
        return descriptions.get(strategy, '')
    
    async def _save_analysis(self, pool_address: str, analysis: Dict):
        """Salva análise no banco"""
        try:
            # Atualizar pool com score e recomendação
            update_data = {
                'score': analysis['score'],
                'recommendation': analysis['recommendation'],
                'last_analyzed': analysis['analyzed_at']
            }
            
            self.supabase.table('pools').update(update_data).eq('address', pool_address).execute()
            
            # Salvar ranges como JSON na pool
            ranges_json = json.dumps(analysis['ranges'])
            simulations_json = json.dumps(analysis['simulations'])
            
            self.supabase.table('pools').update({
                'ranges_data': ranges_json,
                'simulations_data': simulations_json,
                'explanation': analysis['explanation']
            }).eq('address', pool_address).execute()
            
            logger.info(f"✅ Análise salva para pool {pool_address}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar análise: {str(e)}")

async def analyze_all_pools(supabase_client):
    """Analisa todas as pools do banco"""
    analyzer = PoolAnalyzer(supabase_client)
    
    # Buscar todas as pools
    result = supabase_client.table('pools').select('address').execute()
    
    if not result.data:
        logger.warning("Nenhuma pool para analisar")
        return []
    
    analyses = []
    for pool in result.data:
        analysis = await analyzer.analyze_pool(pool['address'])
        if analysis:
            analyses.append(analysis)
    
    return analyses
