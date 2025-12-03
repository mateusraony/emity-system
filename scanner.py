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
        # URL atualizada do Graph para Arbitrum
        self.graph_url = "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-arbitrum"
        self.gecko_base = "https://api.geckoterminal.com/api/v2"
        self.dexscreener_base = "https://api.dexscreener.com/latest/dex"
        
        # Filtros institucionais ajustados para encontrar mais pools
        self.MIN_TVL = 10000  # Reduzido para $10k para teste
        self.MIN_VOLUME_24H = 1000  # Reduzido para $1k para teste
        self.MIN_FEE_APR = 1  # 1% anual mínimo
        
        # Top tokens institucionais no Arbitrum
        self.INSTITUTIONAL_TOKENS = {
            'WETH', 'USDC', 'USDT', 'ARB', 'WBTC', 'DAI', 
            'GMX', 'LINK', 'UNI', 'AAVE', 'CRV', 'SUSHI',
            'USDC.e', 'MAGIC', 'RDNT', 'DPX', 'GRAIL'
        }
    
    async def scan_pools(self) -> List[Dict]:
        """Executa scan completo de pools"""
        logger.info("🔍 Iniciando scan de pools Uniswap v3 Arbitrum...")
        
        try:
            # Buscar pools do The Graph
            pools = await self._fetch_graph_pools()
            
            if not pools:
                logger.warning("Nenhuma pool encontrada no The Graph, tentando API alternativa...")
                # Tentar buscar via DexScreener como fallback
                pools = await self._fetch_dexscreener_pools()
            
            if not pools:
                logger.warning("Nenhuma pool encontrada em nenhuma API")
                return []
            
            logger.info(f"📊 {len(pools)} pools brutas encontradas")
            
            # Filtrar pools institucionais
            filtered_pools = self._filter_institutional_pools(pools)
            logger.info(f"✅ {len(filtered_pools)} pools passaram no filtro institucional")
            
            if not filtered_pools:
                # Se nenhuma passou, pegar as top 5 pools sem filtro institucional
                logger.warning("Nenhuma pool passou filtro institucional, pegando top 5...")
                filtered_pools = pools[:5] if len(pools) > 5 else pools
            
            # Enriquecer com dados de preço
            enriched_pools = await self._enrich_with_price_data(filtered_pools)
            
            # Calcular métricas
            analyzed_pools = self._calculate_metrics(enriched_pools)
            
            # Salvar no banco
            await self._save_to_database(analyzed_pools)
            
            logger.info(f"✅ Scan completo! {len(analyzed_pools)} pools analisadas")
            return analyzed_pools
            
        except Exception as e:
            logger.error(f"❌ Erro no scan: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    async def _fetch_graph_pools(self) -> List[Dict]:
        """Busca pools do The Graph"""
        # Query mais simples para garantir resultados
        query = """
        {
          pools(
            first: 50
            orderBy: totalValueLockedUSD
            orderDirection: desc
            where: {
              totalValueLockedUSD_gt: "1000"
            }
          ) {
            id
            token0 {
              id
              symbol
              name
              decimals
            }
            token1 {
              id
              symbol
              name
              decimals
            }
            feeTier
            liquidity
            sqrtPrice
            tick
            totalValueLockedUSD
            totalValueLockedToken0
            totalValueLockedToken1
            volumeUSD
            feesUSD
            txCount
            token0Price
            token1Price
          }
        }
        """
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.graph_url,
                    json={'query': query},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        pools = data.get('data', {}).get('pools', [])
                        logger.info(f"Graph API retornou {len(pools)} pools")
                        return pools
                    else:
                        logger.error(f"Graph API error: {response.status}")
                        text = await response.text()
                        logger.error(f"Response: {text[:500]}")
                        return []
        except Exception as e:
            logger.error(f"Erro ao buscar pools do Graph: {str(e)}")
            return []
    
    async def _fetch_dexscreener_pools(self) -> List[Dict]:
        """Busca pools do DexScreener como fallback"""
        try:
            url = f"{self.dexscreener_base}/pairs/arbitrum/uniswapv3"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        pairs = data.get('pairs', [])
                        
                        # Converter formato DexScreener para nosso formato
                        pools = []
                        for pair in pairs[:20]:  # Pegar top 20
                            pool = {
                                'id': pair.get('pairAddress', ''),
                                'token0': {
                                    'id': pair.get('baseToken', {}).get('address', ''),
                                    'symbol': pair.get('baseToken', {}).get('symbol', ''),
                                    'name': pair.get('baseToken', {}).get('name', ''),
                                    'decimals': 18
                                },
                                'token1': {
                                    'id': pair.get('quoteToken', {}).get('address', ''),
                                    'symbol': pair.get('quoteToken', {}).get('symbol', ''),
                                    'name': pair.get('quoteToken', {}).get('name', ''),
                                    'decimals': 18
                                },
                                'feeTier': 3000,  # Default 0.3%
                                'totalValueLockedUSD': pair.get('liquidity', {}).get('usd', 0),
                                'volumeUSD': pair.get('volume', {}).get('h24', 0),
                                'feesUSD': 0,
                                'token0Price': pair.get('priceUsd', 0),
                                'token1Price': 0
                            }
                            pools.append(pool)
                        
                        logger.info(f"DexScreener retornou {len(pools)} pools")
                        return pools
                    else:
                        logger.error(f"DexScreener API error: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"Erro ao buscar pools do DexScreener: {str(e)}")
            return []
    
    def _filter_institutional_pools(self, pools: List[Dict]) -> List[Dict]:
        """Filtra apenas pools institucionais"""
        filtered = []
        
        for pool in pools:
            try:
                # Verificar tokens
                token0_symbol = pool.get('token0', {}).get('symbol', '').upper()
                token1_symbol = pool.get('token1', {}).get('symbol', '').upper()
                
                # Verificar TVL e volume
                tvl = float(pool.get('totalValueLockedUSD', 0))
                volume = float(pool.get('volumeUSD', 0))
                
                # Critérios mais flexíveis para teste
                if tvl >= self.MIN_TVL and volume >= self.MIN_VOLUME_24H:
                    filtered.append(pool)
                    logger.info(f"✅ Pool {token0_symbol}/{token1_symbol} - TVL: ${tvl:,.0f}, Volume: ${volume:,.0f}")
                
            except Exception as e:
                logger.warning(f"Erro ao filtrar pool: {str(e)}")
                continue
        
        return filtered
    
    async def _enrich_with_price_data(self, pools: List[Dict]) -> List[Dict]:
        """Enriquece pools com dados de preço atuais"""
        enriched = []
        
        for pool in pools:
            try:
                # Usar dados existentes do Graph/DexScreener
                pool['current_price'] = float(pool.get('token0Price', 1))
                pool['price_change_24h'] = 0  # Seria necessário histórico
                pool['volume_24h'] = float(pool.get('volumeUSD', 0))
                
                enriched.append(pool)
                
            except Exception as e:
                logger.warning(f"Erro ao enriquecer pool: {str(e)}")
                enriched.append(pool)
        
        return enriched
    
    def _calculate_metrics(self, pools: List[Dict]) -> List[Dict]:
        """Calcula métricas institucionais para cada pool"""
        analyzed = []
        
        for pool in pools:
            try:
                # Dados básicos
                pool_data = {
                    'address': pool['id'].lower(),
                    'token0_symbol': pool['token0']['symbol'],
                    'token0_address': pool['token0']['id'].lower(),
                    'token1_symbol': pool['token1']['symbol'],
                    'token1_address': pool['token1']['id'].lower(),
                    'fee_tier': int(pool.get('feeTier', 3000)) / 10000,  # Convert to percentage
                    'tvl_usd': float(pool.get('totalValueLockedUSD', 0)),
                    'volume_24h': float(pool.get('volume_24h', pool.get('volumeUSD', 0))),
                    'fees_24h': float(pool.get('feesUSD', 0)),
                    'current_tick': int(pool.get('tick', 0)) if pool.get('tick') else 0,
                    'current_price': float(pool.get('current_price', 1)),
                    'price_change_24h': float(pool.get('price_change_24h', 0)),
                }
                
                # Calcular APR de fees
                if pool_data['tvl_usd'] > 0 and pool_data['volume_24h'] > 0:
                    # Estimar fees baseado no volume e fee tier
                    estimated_daily_fees = pool_data['volume_24h'] * pool_data['fee_tier'] / 100
                    daily_fee_rate = estimated_daily_fees / pool_data['tvl_usd']
                    pool_data['fee_apr'] = daily_fee_rate * 365 * 100  # em %
                else:
                    pool_data['fee_apr'] = 0
                
                # Se não temos fees_24h, estimar
                if pool_data['fees_24h'] == 0 and pool_data['volume_24h'] > 0:
                    pool_data['fees_24h'] = pool_data['volume_24h'] * pool_data['fee_tier'] / 100
                
                # Calcular volatilidade (simplificada)
                pool_data['volatility'] = abs(pool_data['price_change_24h']) if pool_data['price_change_24h'] else 10
                
                # Calcular IL estimada (fórmula simplificada)
                volatility_factor = pool_data['volatility'] / 100
                pool_data['il_7d'] = volatility_factor * 7 * 2  # % em 7 dias
                pool_data['il_30d'] = volatility_factor * 30 * 2  # % em 30 dias
                
                # Score inicial (será refinado pelo analyzer)
                pool_data['score'] = self._calculate_initial_score(pool_data)
                
                # Timestamps
                pool_data['last_updated'] = datetime.utcnow().isoformat()
                pool_data['created_at'] = datetime.utcnow().isoformat()
                
                analyzed.append(pool_data)
                
                logger.info(f"📊 Analisada: {pool_data['token0_symbol']}/{pool_data['token1_symbol']} - Score: {pool_data['score']}")
                
            except Exception as e:
                logger.warning(f"Erro ao calcular métricas para pool: {str(e)}")
                continue
        
        return analyzed
    
    def _calculate_initial_score(self, pool: Dict) -> int:
        """Calcula score inicial baseado em métricas"""
        score = 50  # Base
        
        # TVL Score (0-20 pontos)
        if pool['tvl_usd'] > 1000000:
            score += 20
        elif pool['tvl_usd'] > 500000:
            score += 15
        elif pool['tvl_usd'] > 100000:
            score += 10
        elif pool['tvl_usd'] > 50000:
            score += 5
        
        # Volume Score (0-15 pontos)
        if pool['volume_24h'] > 500000:
            score += 15
        elif pool['volume_24h'] > 100000:
            score += 10
        elif pool['volume_24h'] > 50000:
            score += 5
        
        # Fee APR Score (0-15 pontos)
        if pool['fee_apr'] > 50:
            score += 15
        elif pool['fee_apr'] > 30:
            score += 10
        elif pool['fee_apr'] > 15:
            score += 5
        
        # Penalidades
        if pool['volatility'] > 20:  # Alta volatilidade
            score -= 10
        if pool['il_7d'] > 5:  # IL alta
            score -= 5
        
        return max(0, min(100, score))
    
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
