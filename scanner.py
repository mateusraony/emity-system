"""
Scanner de Pools - EMITY System
Busca e analisa pools reais do Uniswap v3 no Arbitrum
"""

import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from decimal import Decimal
import math

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UniswapV3Scanner:
    """Scanner para pools Uniswap v3 no Arbitrum"""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        
        # APIs configuradas
        self.dexscreener_base = "https://api.dexscreener.com/latest/dex"
        
        # Pools conhecidas do Uniswap v3 Arbitrum (para garantir dados)
        self.KNOWN_POOLS = [
            "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",  # WETH/USDC.e 0.05%
            "0xC473e2aEE3441BaFEb62433F3d7E4d3d4B906a0b",  # WETH/USDC 0.05%
            "0x641C00A822e8b671738d32a431a4Fb6074E5c79d",  # WETH/USDT 0.05%
            "0x0E4831319A50228B9e450861297aB92dee15B44F",  # WETH/ARB 0.05%
            "0x92c63d0e701CAAe670C9415d91C474F686298f00",  # USDC/USDT 0.01%
            "0x8C9D230D45d6CfeE39a6680Fb7CB7E8DE7Ea8E71",  # WETH/DAI 0.3%
            "0x35218a1cbaC5Bbc3E57fd9Bd38219D37571b3537",  # WETH/WBTC 0.05%
            "0xfAe941346Ac34908b8D7d000f86056A18049146E",  # ARB/USDC 0.3%
            "0xcDa53B1F66614552F834cEeF361A8D12a0B8DaD8",  # ARB/USDT 0.05%
            "0x446BF9748B4eA044dd759d9B9311C70491dF8F29",  # GMX/WETH 0.3%
        ]
        
        # Filtros ajustados
        self.MIN_TVL = 5000  # $5k mínimo
        self.MIN_VOLUME_24H = 500  # $500 mínimo
    
    async def scan_pools(self) -> List[Dict]:
        """Executa scan completo de pools"""
        logger.info("🔍 Iniciando scan de pools Uniswap v3 Arbitrum...")
        
        try:
            all_pools = []
            
            # 1. Buscar via DexScreener (mais confiável)
            dex_pools = await self._fetch_dexscreener_pairs()
            if dex_pools:
                all_pools.extend(dex_pools)
                logger.info(f"✅ {len(dex_pools)} pools encontradas via DexScreener")
            
            # 2. Adicionar pools conhecidas se não temos dados suficientes
            if len(all_pools) < 5:
                logger.info("📊 Buscando pools conhecidas para garantir dados...")
                known_pools = await self._fetch_known_pools()
                all_pools.extend(known_pools)
            
            if not all_pools:
                logger.error("❌ Nenhuma pool encontrada")
                # Criar pools mock para teste
                all_pools = self._create_mock_pools()
            
            # Filtrar duplicatas
            seen = set()
            unique_pools = []
            for pool in all_pools:
                if pool['address'] not in seen:
                    seen.add(pool['address'])
                    unique_pools.append(pool)
            
            # Calcular métricas
            analyzed_pools = self._calculate_metrics(unique_pools[:20])  # Limitar a 20 pools
            
            # Salvar no banco
            await self._save_to_database(analyzed_pools)
            
            logger.info(f"✅ Scan completo! {len(analyzed_pools)} pools analisadas")
            return analyzed_pools
            
        except Exception as e:
            logger.error(f"❌ Erro no scan: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    async def _fetch_dexscreener_pairs(self) -> List[Dict]:
        """Busca pares do DexScreener"""
        try:
            # Buscar top pairs do Arbitrum
            url = f"{self.dexscreener_base}/pairs/arbitrum"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        pairs = data.get('pairs', []) if data else []
                        
                        # Filtrar apenas Uniswap v3
                        uni_v3_pairs = [
                            p for p in pairs 
                            if 'uniswap' in p.get('dexId', '').lower() and
                            p.get('liquidity', {}).get('usd', 0) > self.MIN_TVL
                        ][:10]  # Top 10
                        
                        pools = []
                        for pair in uni_v3_pairs:
                            pools.append(self._parse_dexscreener_pair(pair))
                        
                        return pools
                    else:
                        logger.error(f"DexScreener API error: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"Erro ao buscar do DexScreener: {str(e)}")
            return []
    
    async def _fetch_known_pools(self) -> List[Dict]:
        """Busca dados das pools conhecidas"""
        pools = []
        
        for address in self.KNOWN_POOLS[:5]:  # Pegar apenas 5 pools conhecidas
            try:
                url = f"{self.dexscreener_base}/pairs/arbitrum/{address}"
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            pair = data.get('pair')
                            if pair:
                                pools.append(self._parse_dexscreener_pair(pair))
                                logger.info(f"✅ Pool conhecida adicionada: {pair.get('baseToken', {}).get('symbol')}/{pair.get('quoteToken', {}).get('symbol')}")
            except Exception as e:
                logger.warning(f"Erro ao buscar pool {address}: {str(e)}")
                continue
        
        return pools
    
    def _parse_dexscreener_pair(self, pair: Dict) -> Dict:
        """Converte formato DexScreener para nosso formato"""
        return {
            'address': pair.get('pairAddress', ''),
            'token0_symbol': pair.get('baseToken', {}).get('symbol', 'TOKEN0'),
            'token0_address': pair.get('baseToken', {}).get('address', ''),
            'token1_symbol': pair.get('quoteToken', {}).get('symbol', 'TOKEN1'),
            'token1_address': pair.get('quoteToken', {}).get('address', ''),
            'fee_tier': 0.3,  # Default 0.3% para Uniswap v3
            'tvl_usd': float(pair.get('liquidity', {}).get('usd', 0)),
            'volume_24h': float(pair.get('volume', {}).get('h24', 0)),
            'current_price': float(pair.get('priceUsd', 0)),
            'price_change_24h': float(pair.get('priceChange', {}).get('h24', 0)),
            'fees_24h': float(pair.get('volume', {}).get('h24', 0)) * 0.003  # Estimado 0.3%
        }
    
    def _create_mock_pools(self) -> List[Dict]:
        """Cria pools mock para teste quando APIs falham"""
        logger.warning("⚠️ Usando dados mock para demonstração")
        
        mock_pools = [
            {
                'address': '0xc31e54c7a869b9fcbecc14363cf510d1c41fa443',
                'token0_symbol': 'WETH',
                'token0_address': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
                'token1_symbol': 'USDC',
                'token1_address': '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8',
                'fee_tier': 0.05,
                'tvl_usd': 15000000,
                'volume_24h': 8500000,
                'current_price': 3450.50,
                'price_change_24h': 2.5,
                'fees_24h': 4250
            },
            {
                'address': '0x0e4831319a50228b9e450861297ab92dee15b44f',
                'token0_symbol': 'WETH',
                'token0_address': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
                'token1_symbol': 'ARB',
                'token1_address': '0x912ce59144191c1204e64559fe8253a0e49e6548',
                'fee_tier': 0.05,
                'tvl_usd': 8500000,
                'volume_24h': 5200000,
                'current_price': 1.85,
                'price_change_24h': -1.2,
                'fees_24h': 2600
            },
            {
                'address': '0x641c00a822e8b671738d32a431a4fb6074e5c79d',
                'token0_symbol': 'WETH',
                'token0_address': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
                'token1_symbol': 'USDT',
                'token1_address': '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9',
                'fee_tier': 0.05,
                'tvl_usd': 6200000,
                'volume_24h': 3800000,
                'current_price': 3448.20,
                'price_change_24h': 2.3,
                'fees_24h': 1900
            },
            {
                'address': '0xfae941346ac34908b8d7d000f86056a18049146e',
                'token0_symbol': 'ARB',
                'token0_address': '0x912ce59144191c1204e64559fe8253a0e49e6548',
                'token1_symbol': 'USDC',
                'token1_address': '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8',
                'fee_tier': 0.3,
                'tvl_usd': 4100000,
                'volume_24h': 2900000,
                'current_price': 1.85,
                'price_change_24h': -1.5,
                'fees_24h': 8700
            },
            {
                'address': '0x446bf9748b4ea044dd759d9b9311c70491df8f29',
                'token0_symbol': 'GMX',
                'token0_address': '0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a',
                'token1_symbol': 'WETH',
                'token1_address': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
                'fee_tier': 0.3,
                'tvl_usd': 2800000,
                'volume_24h': 1500000,
                'current_price': 42.30,
                'price_change_24h': 3.8,
                'fees_24h': 4500
            }
        ]
        
        return mock_pools
    
    def _calculate_metrics(self, pools: List[Dict]) -> List[Dict]:
        """Calcula métricas institucionais para cada pool"""
        analyzed = []
        
        for pool in pools:
            try:
                # Garantir que temos todos os campos necessários
                pool_data = {
                    'address': pool.get('address', '').lower(),
                    'token0_symbol': pool.get('token0_symbol', 'TOKEN0'),
                    'token0_address': pool.get('token0_address', '').lower(),
                    'token1_symbol': pool.get('token1_symbol', 'TOKEN1'),
                    'token1_address': pool.get('token1_address', '').lower(),
                    'fee_tier': float(pool.get('fee_tier', 0.3)),
                    'tvl_usd': float(pool.get('tvl_usd', 0)),
                    'volume_24h': float(pool.get('volume_24h', 0)),
                    'fees_24h': float(pool.get('fees_24h', 0)),
                    'current_tick': 0,  # Não disponível via DexScreener
                    'current_price': float(pool.get('current_price', 1)),
                    'price_change_24h': float(pool.get('price_change_24h', 0)),
                }
                
                # Calcular APR de fees
                if pool_data['tvl_usd'] > 0:
                    if pool_data['fees_24h'] > 0:
                        daily_fee_rate = pool_data['fees_24h'] / pool_data['tvl_usd']
                    else:
                        # Estimar com base no volume
                        daily_fee_rate = (pool_data['volume_24h'] * pool_data['fee_tier'] / 100) / pool_data['tvl_usd']
                    
                    pool_data['fee_apr'] = daily_fee_rate * 365 * 100  # em %
                else:
                    pool_data['fee_apr'] = 0
                
                # Volatilidade baseada na mudança de preço
                pool_data['volatility'] = abs(pool_data['price_change_24h'])
                
                # IL estimada
                volatility_factor = pool_data['volatility'] / 100
                pool_data['il_7d'] = min(volatility_factor * 7 * 1.5, 10)  # Max 10%
                pool_data['il_30d'] = min(volatility_factor * 30 * 1.5, 25)  # Max 25%
                
                # Score
                pool_data['score'] = self._calculate_score(pool_data)
                
                # Recomendação inicial
                if pool_data['score'] >= 70:
                    pool_data['recommendation'] = f"💎 FORTE COMPRA - {pool_data['token0_symbol']}/{pool_data['token1_symbol']} com APR de {pool_data['fee_apr']:.1f}%"
                elif pool_data['score'] >= 50:
                    pool_data['recommendation'] = f"✅ COMPRA - {pool_data['token0_symbol']}/{pool_data['token1_symbol']} com potencial de {pool_data['fee_apr']:.1f}% APR"
                else:
                    pool_data['recommendation'] = f"⚠️ AVALIAR - {pool_data['token0_symbol']}/{pool_data['token1_symbol']} requer análise adicional"
                
                # Explicação
                pool_data['explanation'] = self._generate_explanation(pool_data)
                
                # Timestamps
                pool_data['last_updated'] = datetime.utcnow().isoformat()
                pool_data['created_at'] = datetime.utcnow().isoformat()
                
                # Ranges mock (será refinado pelo analyzer)
                pool_data['ranges_data'] = json.dumps(self._generate_mock_ranges(pool_data))
                pool_data['simulations_data'] = json.dumps(self._generate_mock_simulations(pool_data))
                
                analyzed.append(pool_data)
                
                logger.info(f"✅ {pool_data['token0_symbol']}/{pool_data['token1_symbol']} - TVL: ${pool_data['tvl_usd']:,.0f} - Score: {pool_data['score']}")
                
            except Exception as e:
                logger.warning(f"Erro ao analisar pool: {str(e)}")
                continue
        
        return analyzed
    
    def _calculate_score(self, pool: Dict) -> int:
        """Calcula score institucional"""
        score = 40  # Base
        
        # TVL (0-25)
        if pool['tvl_usd'] > 10000000:
            score += 25
        elif pool['tvl_usd'] > 5000000:
            score += 20
        elif pool['tvl_usd'] > 1000000:
            score += 15
        elif pool['tvl_usd'] > 100000:
            score += 10
        elif pool['tvl_usd'] > 10000:
            score += 5
        
        # Volume (0-20)
        if pool['volume_24h'] > 5000000:
            score += 20
        elif pool['volume_24h'] > 1000000:
            score += 15
        elif pool['volume_24h'] > 100000:
            score += 10
        elif pool['volume_24h'] > 10000:
            score += 5
        
        # APR (0-20)
        if pool['fee_apr'] > 100:
            score += 20
        elif pool['fee_apr'] > 50:
            score += 15
        elif pool['fee_apr'] > 20:
            score += 10
        elif pool['fee_apr'] > 10:
            score += 5
        
        # Volatilidade (0-15)
        if pool['volatility'] < 2:
            score += 15
        elif pool['volatility'] < 5:
            score += 10
        elif pool['volatility'] < 10:
            score += 5
        
        return min(100, max(0, score))
    
    def _generate_explanation(self, pool: Dict) -> str:
        """Gera explicação do score"""
        pair = f"{pool['token0_symbol']}/{pool['token1_symbol']}"
        
        if pool['score'] >= 80:
            return f"🌟 Pool {pair} EXCELENTE - TVL sólida de ${pool['tvl_usd']:,.0f}, APR atrativa de {pool['fee_apr']:.1f}% e baixo risco."
        elif pool['score'] >= 60:
            return f"✅ Pool {pair} BOA - TVL de ${pool['tvl_usd']:,.0f}, APR de {pool['fee_apr']:.1f}% com risco moderado."
        elif pool['score'] >= 40:
            return f"⚠️ Pool {pair} MODERADA - TVL de ${pool['tvl_usd']:,.0f}, considerar relação risco/retorno."
        else:
            return f"❌ Pool {pair} BAIXA - Métricas insuficientes para recomendação institucional."
    
    def _generate_mock_ranges(self, pool: Dict) -> Dict:
        """Gera ranges mock para teste"""
        price = pool['current_price']
        return {
            'defensive': {
                'min_price': price * 0.85,
                'max_price': price * 1.15,
                'spread_percent': 30,
                'strategy': 'defensive',
                'description': '🛡️ Conservador - Range amplo para menor risco'
            },
            'optimized': {
                'min_price': price * 0.92,
                'max_price': price * 1.08,
                'spread_percent': 16,
                'strategy': 'optimized',
                'description': '⚖️ Balanceado - Equilibrio entre risco e retorno'
            },
            'aggressive': {
                'min_price': price * 0.96,
                'max_price': price * 1.04,
                'spread_percent': 8,
                'strategy': 'aggressive',
                'description': '🚀 Agressivo - Range estreito para máximo retorno'
            }
        }
    
    def _generate_mock_simulations(self, pool: Dict) -> Dict:
        """Gera simulações mock para teste"""
        apr = pool['fee_apr']
        il = pool['il_7d']
        
        return {
            'defensive': {
                '7d': {
                    'time_in_range': 95,
                    'fees_collected': apr * 7 / 365 * 0.8,
                    'impermanent_loss': il * 0.5,
                    'net_return': (apr * 7 / 365 * 0.8) - (il * 0.5),
                    'gas_cost': 5,
                    'net_after_gas': (apr * 7 / 365 * 0.8) - (il * 0.5) - 0.5
                },
                '30d': {
                    'time_in_range': 90,
                    'fees_collected': apr * 30 / 365 * 0.8,
                    'impermanent_loss': pool['il_30d'] * 0.5,
                    'net_return': (apr * 30 / 365 * 0.8) - (pool['il_30d'] * 0.5),
                    'gas_cost': 5,
                    'net_after_gas': (apr * 30 / 365 * 0.8) - (pool['il_30d'] * 0.5) - 0.5
                }
            },
            'optimized': {
                '7d': {
                    'time_in_range': 85,
                    'fees_collected': apr * 7 / 365,
                    'impermanent_loss': il * 0.7,
                    'net_return': (apr * 7 / 365) - (il * 0.7),
                    'gas_cost': 5,
                    'net_after_gas': (apr * 7 / 365) - (il * 0.7) - 0.5
                },
                '30d': {
                    'time_in_range': 80,
                    'fees_collected': apr * 30 / 365,
                    'impermanent_loss': pool['il_30d'] * 0.7,
                    'net_return': (apr * 30 / 365) - (pool['il_30d'] * 0.7),
                    'gas_cost': 5,
                    'net_after_gas': (apr * 30 / 365) - (pool['il_30d'] * 0.7) - 0.5
                }
            },
            'aggressive': {
                '7d': {
                    'time_in_range': 70,
                    'fees_collected': apr * 7 / 365 * 1.3,
                    'impermanent_loss': il,
                    'net_return': (apr * 7 / 365 * 1.3) - il,
                    'gas_cost': 5,
                    'net_after_gas': (apr * 7 / 365 * 1.3) - il - 0.5
                },
                '30d': {
                    'time_in_range': 65,
                    'fees_collected': apr * 30 / 365 * 1.3,
                    'impermanent_loss': pool['il_30d'],
                    'net_return': (apr * 30 / 365 * 1.3) - pool['il_30d'],
                    'gas_cost': 5,
                    'net_after_gas': (apr * 30 / 365 * 1.3) - pool['il_30d'] - 0.5
                }
            }
        }
    
    async def _save_to_database(self, pools: List[Dict]):
        """Salva pools no Supabase"""
        try:
            for pool in pools:
                # Verificar se já existe
                existing = self.supabase.table('pools').select('*').eq('address', pool['address']).execute()
                
                if existing.data:
                    # Atualizar
                    self.supabase.table('pools').update(pool).eq('address', pool['address']).execute()
                    logger.info(f"📝 Pool {pool['token0_symbol']}/{pool['token1_symbol']} atualizada")
                else:
                    # Inserir
                    self.supabase.table('pools').insert(pool).execute()
                    logger.info(f"✅ Nova pool {pool['token0_symbol']}/{pool['token1_symbol']} adicionada")
                    
        except Exception as e:
            logger.error(f"Erro ao salvar no banco: {str(e)}")

async def run_scanner(supabase_client):
    """Função auxiliar para executar o scanner"""
    scanner = UniswapV3Scanner(supabase_client)
    return await scanner.scan_pools()
