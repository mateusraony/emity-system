"""
EMITY System - Motor de Risco e Position Sizing
Fase 2: Gestão de capital e regras institucionais
"""
import logging
import json
from typing import Dict, List, Optional, Tuple
from decimal import Decimal, ROUND_DOWN

logger = logging.getLogger(__name__)

class RiskEngine:
    """Motor de risco institucional para LPs"""
    
    # Constantes de gas (Arbitrum)
    GAS_COST_USD = 5.0  # Custo médio de gas em Arbitrum
    MIN_GAS_MULTIPLIER = 2.0  # Retorno mínimo deve ser 2x o gas
    GAS_WARNING_THRESHOLD = 0.10  # Avisar se gas > 10% do retorno
    
    # Limites por perfil de risco
    RISK_PROFILES = {
        'conservador': {
            'max_position_pct': 20.0,
            'min_score': 70,
            'max_il_tolerance': 5.0,
            'range_type': 'defensivo'
        },
        'moderado': {
            'max_position_pct': 30.0,
            'min_score': 60,
            'max_il_tolerance': 10.0,
            'range_type': 'otimizado'
        },
        'agressivo': {
            'max_position_pct': 40.0,
            'min_score': 50,
            'max_il_tolerance': 15.0,
            'range_type': 'agressivo'
        }
    }
    
    def __init__(self, config: Dict):
        """Inicializa o motor com configurações do usuário"""
        self.capital_total = float(config.get('capital_total', 10000))
        self.perfil_risco = config.get('perfil_risco', 'conservador')
        self.max_positions = int(config.get('max_positions', 3))
        self.stop_loss = float(config.get('stop_loss', 10.0))
        self.max_position_size = float(config.get('max_position_size', 30.0))
        self.min_score = int(config.get('min_score', 60))
        self.gas_multiplier = float(config.get('gas_multiplier', 2.0))
        
        # Aplicar limites do perfil
        profile = self.RISK_PROFILES.get(self.perfil_risco, self.RISK_PROFILES['conservador'])
        self.profile_limits = profile

    # ------------------------
    # Helpers internos
    # ------------------------
    def _get_pool_address(self, pool: Dict) -> Optional[str]:
        """Retorna o identificador único da pool, independente do formato."""
        return pool.get('pool_address') or pool.get('address')

    def _get_pair_label(self, pool: Dict) -> str:
        """Retorna o par em formato legível."""
        if pool.get('pair'):
            return pool['pair']
        t0 = pool.get('token0_symbol', 'TOKEN0')
        t1 = pool.get('token1_symbol', 'TOKEN1')
        return f"{t0}/{t1}"

    def _extract_simulation_7d(self, pool_data: Dict) -> Dict:
        """
        Extrai uma visão consolidada de simulação 7d da pool.
        Aceita:
        - pool_data['simulation_7d'] já pronto
        - pool_data['sim_7d']
        - blob pool_data['simulations'] ou pool_data['simulations_data']
          no formato gerado pelo analyzer.py (defensive/optimized/aggressive).
        """
        # Caso já exista um simulation_7d estruturado, usa direto
        sim = pool_data.get('simulation_7d')
        if isinstance(sim, dict):
            return sim

        sim = pool_data.get('sim_7d')
        if isinstance(sim, dict):
            return sim

        # Tentar extrair do blob de simulations
        sims_blob = pool_data.get('simulations') or pool_data.get('simulations_data')
        if isinstance(sims_blob, str):
            try:
                sims = json.loads(sims_blob)
            except Exception:
                sims = None
        else:
            sims = sims_blob

        if isinstance(sims, dict):
            best = None  # (net_after_gas, data_7d)
            for key in ('defensive', 'optimized', 'aggressive'):
                strat = sims.get(key)
                if not isinstance(strat, dict):
                    continue
                data_7d = strat.get('7d') or {}
                if not isinstance(data_7d, dict):
                    continue

                net_after = data_7d.get('net_after_gas')
                if net_after is None:
                    net_after = data_7d.get('net_return')

                try:
                    net_after_val = float(net_after)
                except (TypeError, ValueError):
                    continue

                # Escolher o melhor net_after_gas (já descontando gas)
                if best is None or net_after_val > best[0]:
                    best = (net_after_val, data_7d)

            if best is not None:
                _, data_7d = best
                # IL em %, campo do analyzer é "impermanent_loss"
                il = data_7d.get('impermanent_loss', 0)
                try:
                    il_val = float(il)
                except (TypeError, ValueError):
                    il_val = 0.0

                time_in_range = data_7d.get('time_in_range', 0)
                try:
                    time_in_range_val = float(time_in_range)
                except (TypeError, ValueError):
                    time_in_range_val = 0.0

                net_ret = data_7d.get('net_return', best[0])
                try:
                    net_ret_val = float(net_ret)
                except (TypeError, ValueError):
                    net_ret_val = float(best[0])

                return {
                    'net_return': net_ret_val,          # % estimado (antes de gas)
                    'net_after_gas': best[0],          # % estimado (após gas)
                    'time_in_range': time_in_range_val,
                    'il_percentage': il_val            # % de IL estimada
                }

        # Sem dados suficientes
        return {}
    
    def calculate_position_size(self, pool_data: Dict, override_pct: Optional[float] = None) -> Dict:
        """
        Calcula o tamanho da posição para uma pool
        Retorna valores em % e USDT sincronizados
        """
        # Score da pool
        score = pool_data.get('score', 0)
        
        # Se score muito baixo, não operar
        if score < self.min_score:
            return {
                'can_operate': False,
                'reason': f'Score {score} abaixo do mínimo {self.min_score}',
                'size_pct': 0,
                'size_usdt': 0,
                'warnings': []
            }
        
        # Calcular tamanho base pela qualidade da pool
        if override_pct:
            base_pct = min(override_pct, self.max_position_size)
        else:
            # Escala linear: score 60->10%, score 100->max%
            score_factor = (score - 60) / 40  # 0 a 1
            base_pct = 10 + (self.profile_limits['max_position_pct'] - 10) * score_factor
        
        # Aplicar limite máximo
        size_pct = min(base_pct, self.max_position_size, self.profile_limits['max_position_pct'])
        size_usdt = self.capital_total * (size_pct / 100)
        
        # Arredondar para baixo (conservador)
        size_usdt = float(Decimal(str(size_usdt)).quantize(Decimal('0.01'), rounding=ROUND_DOWN))
        
        # Validar gas
        gas_check = self.validate_gas_cost(size_usdt, pool_data)
        
        return {
            'can_operate': gas_check['viable'],
            'reason': gas_check.get('reason', 'OK'),
            'size_pct': round(size_pct, 2),
            'size_usdt': size_usdt,
            'gas_cost': self.GAS_COST_USD,
            'warnings': gas_check.get('warnings', [])
        }
    
    def validate_gas_cost(self, position_size: float, pool_data: Dict) -> Dict:
        """Valida se vale a pena pagar o gas para essa posição"""
        # Estimar retorno esperado (7 dias) em USD
        sim_7d = self._extract_simulation_7d(pool_data)
        estimated_return_usd = 0.0

        if sim_7d:
            # Valores vindo do analyzer são em %, converter para USD
            net_pct = sim_7d.get('net_after_gas')
            if net_pct is None:
                net_pct = sim_7d.get('net_return', 0)
            try:
                net_pct_val = float(net_pct)
            except (TypeError, ValueError):
                net_pct_val = 0.0
            estimated_return_usd = position_size * (net_pct_val / 100.0)
        
        # Se não tem simulação, estimar por APR
        if estimated_return_usd <= 0 and pool_data.get('apr_7d'):
            try:
                apr_7d = float(pool_data['apr_7d'])
                estimated_return_usd = position_size * (apr_7d / 100.0) * (7.0 / 365.0)
            except (TypeError, ValueError):
                estimated_return_usd = 0.0
        
        warnings = []
        
        # Regra 1: Retorno deve ser pelo menos 2x o gas
        min_return_needed = self.GAS_COST_USD * self.gas_multiplier
        if estimated_return_usd < min_return_needed:
            return {
                'viable': False,
                'reason': f'Retorno estimado ${estimated_return_usd:.2f} < ${min_return_needed:.2f} (gas x{self.gas_multiplier})',
                'warnings': []
            }
        
        # Regra 2: Avisar se gas > 10% do retorno
        gas_pct = (self.GAS_COST_USD / estimated_return_usd) * 100 if estimated_return_usd > 0 else 100
        if gas_pct > self.GAS_WARNING_THRESHOLD * 100:
            warnings.append(f'⚠️ Gas representa {gas_pct:.1f}% do retorno esperado')
        
        return {
            'viable': True,
            'warnings': warnings,
            'gas_percentage': gas_pct
        }
    
    def check_market_conditions(self, pools: List[Dict]) -> Dict:
        """
        Verifica se as condições de mercado permitem operar
        Retorna análise e recomendação
        """
        if not pools:
            return {
                'can_operate': False,
                'reason': 'Nenhuma pool disponível para análise',
                'recommendation': 'Aguardar scanner coletar dados',
                'good_pools': [],
                'market_score': 0
            }
        
        # Filtrar pools viáveis
        good_pools = []
        total_score = 0
        
        for pool in pools:
            score = pool.get('score', 0)
            sim_7d = self._extract_simulation_7d(pool)
            
            # Verificar critérios mínimos
            if score >= self.min_score and sim_7d:
                il_raw = sim_7d.get('il_percentage', sim_7d.get('impermanent_loss', 0))
                try:
                    il_pct = abs(float(il_raw))
                except (TypeError, ValueError):
                    il_pct = 0.0

                net_raw = sim_7d.get('net_after_gas', sim_7d.get('net_return', 0))
                try:
                    net_return = float(net_raw)
                except (TypeError, ValueError):
                    net_return = 0.0
                
                # IL não pode ser maior que o retorno e retorno deve ser positivo
                if il_pct <= self.profile_limits['max_il_tolerance'] and net_return > 0:
                    good_pools.append({
                        'pool_address': self._get_pool_address(pool),
                        'pair': self._get_pair_label(pool),
                        'score': score,
                        'net_return': net_return,
                        'il_percentage': il_pct
                    })
                    total_score += score
        
        # Calcular score médio do mercado
        market_score = (total_score / len(pools)) if pools else 0
        
        # Decisão final
        can_operate = len(good_pools) > 0 and market_score >= 50
        
        if not can_operate:
            if market_score < 50:
                reason = f'Mercado desfavorável (score médio: {market_score:.0f})'
                recommendation = '🔴 NÃO OPERAR HOJE - Aguardar melhores condições'
            elif len(good_pools) == 0:
                reason = 'Nenhuma pool atende aos critérios de risco'
                recommendation = '⚠️ Ajustar perfil de risco ou aguardar'
            else:
                reason = 'Condições incertas'
                recommendation = '⚠️ Operar com cautela reduzida'
        else:
            reason = f'{len(good_pools)} pools viáveis encontradas'
            recommendation = f'✅ Operar nas {min(len(good_pools), self.max_positions)} melhores pools'
        
        return {
            'can_operate': can_operate,
            'reason': reason,
            'recommendation': recommendation,
            'good_pools': good_pools[:self.max_positions],  # Limitar ao máximo de posições
            'market_score': round(market_score, 1),
            'total_opportunities': len(good_pools)
        }
    
    def calculate_portfolio_allocation(self, pools: List[Dict]) -> Dict:
        """
        Calcula alocação ótima para múltiplas pools
        Respeita limites de risco e capital
        """
        # Verificar condições de mercado primeiro
        market_check = self.check_market_conditions(pools)
        
        if not market_check['can_operate']:
            return {
                'can_operate': False,
                'reason': market_check['reason'],
                'recommendation': market_check['recommendation'],
                'allocations': []
            }
        
        # Alocar capital nas melhores pools
        allocations = []
        remaining_capital = self.capital_total
        
        for pool in market_check['good_pools']:
            # Buscar dados completos da pool
            full_pool = next(
                (p for p in pools if self._get_pool_address(p) == pool['pool_address']),
                pool
            )
            
            # Calcular tamanho da posição
            position = self.calculate_position_size(full_pool)
            
            if position['can_operate'] and position['size_usdt'] <= remaining_capital:
                allocations.append({
                    'pool_address': pool['pool_address'],
                    'pair': pool['pair'],
                    'score': pool['score'],
                    'allocation_pct': position['size_pct'],
                    'allocation_usdt': position['size_usdt'],
                    'expected_return': pool['net_return'],
                    'warnings': position.get('warnings', [])
                })
                remaining_capital -= position['size_usdt']
        
        total_allocated = self.capital_total - remaining_capital
        total_allocated_pct = (total_allocated / self.capital_total * 100) if self.capital_total > 0 else 0
        
        return {
            'can_operate': True,
            'reason': f'{len(allocations)} posições alocadas',
            'recommendation': market_check['recommendation'],
            'allocations': allocations,
            'total_allocated_usdt': round(total_allocated, 2),
            'total_allocated_pct': round(total_allocated_pct, 2),
            'remaining_capital': round(remaining_capital, 2),
            'market_score': market_check['market_score']
        }

    def sync_position_values(self, value: float, value_type: str = 'pct') -> Tuple[float, float]:
        """
        Sincroniza valores entre % e USDT
        Retorna (pct, usdt)
        """
        if value_type == 'pct':
            pct = min(value, 100.0)  # Máximo 100%
            usdt = self.capital_total * (pct / 100)
        else:  # value_type == 'usdt'
            usdt = min(value, self.capital_total)  # Máximo = capital total
            pct = (usdt / self.capital_total * 100) if self.capital_total > 0 else 0
        
        return round(pct, 2), round(usdt, 2)

    def validate_stop_loss(self, current_pnl: float, position_size: float) -> Dict:
        """Verifica se deve executar stop loss"""
        if position_size <= 0:
            return {'should_stop': False}
        
        loss_pct = (current_pnl / position_size * 100) if position_size > 0 else 0
        
        if loss_pct <= -self.stop_loss:
            return {
                'should_stop': True,
                'reason': f'Stop Loss atingido: {loss_pct:.2f}%',
                'loss_amount': current_pnl
            }
        
        return {'should_stop': False, 'current_loss_pct': loss_pct}
