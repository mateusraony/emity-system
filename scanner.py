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
        self.graph_url = "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-arbitrum-one"
        self.gecko_base = "https://api.geckoterminal.com/api/v2"
        self.dexscreener_base = "https://api.dexscreener.com/latest/dex"
        
        # Filtros institucionais
        self.MIN_TVL = 50000  # $50k
        self.MIN_VOLUME_24H = 10000  # $10k
        self.MIN_FEE_APR = 5  # 5% anual mínimo
        
        # Top tokens institucionais no Arbitrum
        self.INSTITUTIONAL_TOKENS = {
            'WETH', 'USDC', 'USDT', 'ARB', 'WBTC', 'DAI', 
            'GMX', 'LINK', 'UNI', 'AAVE', 'CRV', 'SUSHI'
        }
    
    async def scan_pools(self) -> List[Dict]:
        """Executa scan completo de pools"""
        logger.info("🔍 Iniciando scan de pools Uniswap v3 Arbitrum...")
        
        try:
            # Buscar pools do The Graph
            pools = await self._fetch_graph_pools()
            
            if not pools:
                logger.warning("Nenhuma pool encontrada no The Graph")
                return []
            
            # Filtrar pools institucionais
            filtered_pools = self._filter_institutional_pools(pools)
            logger.info(f"✅ {len(filtered_pools)} pools passaram no filtro institucional")
            
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
            return []
    
    async def _fetch_graph_pools(self) -> List[Dict]:
        """Busca pools do The Graph"""
        query = """
        {
          pools(
            first: 100
            orderBy: totalValueLockedUSD
            orderDirection: desc
            where: {
              totalValueLockedUSD_gt: "50000"
              volumeUSD_gt: "10000"
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
                        return data.get('data', {}).get('pools', [])
                    else:
                        logger.error(f"Graph API error: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"Erro ao buscar pools do Graph: {str(e)}")
            return []
    
    def _filter_institutional_pools(self, pools: List[Dict]) -> List[Dict]:
        """Filtra apenas pools institucionais"""
        filtered = []
        
        for pool in pools:
            # Verificar tokens
            token0_symbol = pool.get('token0', {}).get('symbol', '').upper()
            token1_symbol = pool.get('token1', {}).get('symbol', '').upper()
            
            # Pelo menos um token deve ser institucional
            if not (token0_symbol in self.INSTITUTIONAL_TOKENS or 
                   token1_symbol in self.INSTITUTIONAL_TOKENS):
                continue
            
            # Verificar TVL e volume
            tvl = float(pool.get('totalValueLockedUSD', 0))
            volume = float(pool.get('volumeUSD', 0))
            
            if tvl < self.MIN_TVL or volume < self.MIN_VOLUME_24H:
                continue
            
            filtered.append(pool)
        
        return filtered
    
    async def _enrich_with_price_data(self, pools: List[Dict]) -> List[Dict]:
        """Enriquece pools com dados de preço atuais"""
        enriched = []
        
        for pool in pools:
            try:
                # Tentar buscar dados do GeckoTerminal
                pool_address = pool['id'].lower()
                gecko_data = await self._fetch_gecko_data(pool_address)
                
                if gecko_data:
                    pool['current_price'] = gecko_data.get('price_usd', 0)
                    pool['price_change_24h'] = gecko_data.get('price_change_24h', 0)
                    pool['volume_24h'] = gecko_data.get('volume_24h', pool.get('volumeUSD', 0))
                else:
                    # Usar dados do Graph como fallback
                    pool['current_price'] = float(pool.get('token0Price', 0))
                    pool['price_change_24h'] = 0
                    pool['volume_24h'] = float(pool.get('volumeUSD', 0))
                
                enriched.append(pool)
                
            except Exception as e:
                logger.warning(f"Erro ao enriquecer pool {pool['id']}: {str(e)}")
                enriched.append(pool)
        
        return enriched
    
    async def _fetch_gecko_data(self, pool_address: str) -> Optional[Dict]:
        """Busca dados do GeckoTerminal"""
        try:
            url = f"{self.gecko_base}/networks/arbitrum/pools/{pool_address}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        pool_data = data.get('data', {}).get('attributes', {})
                        return {
                            'price_usd': float(pool_data.get('base_token_price_usd', 0)),
                            'price_change_24h': float(pool_data.get('price_change_percentage_24h', 0)),
                            'volume_24h': float(pool_data.get('volume_usd_24h', 0))
                        }
                    return None
        except:
            return None
    
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
                    'fee_tier': int(pool['feeTier']) / 10000,  # Convert to percentage
                    'tvl_usd': float(pool.get('totalValueLockedUSD', 0)),
                    'volume_24h': float(pool.get('volume_24h', 0)),
                    'fees_24h': float(pool.get('feesUSD', 0)),
                    'current_tick': int(pool.get('tick', 0)),
                    'current_price': float(pool.get('current_price', 0)),
                    'price_change_24h': float(pool.get('price_change_24h', 0)),
                }
                
                # Calcular APR de fees
                if pool_data['tvl_usd'] > 0:
                    daily_fee_rate = pool_data['fees_24h'] / pool_data['tvl_usd']
                    pool_data['fee_apr'] = daily_fee_rate * 365 * 100  # em %
                else:
                    pool_data['fee_apr'] = 0
                
                # Calcular volatilidade (simplificada)
                pool_data['volatility'] = abs(pool_data['price_change_24h'])
                
                # Calcular IL estimada (fórmula simplificada)
                price_ratio = 1 + (pool_data['price_change_24h'] / 100)
                il_factor = 2 * math.sqrt(price_ratio) / (1 + price_ratio) - 1
                pool_data['il_7d'] = abs(il_factor) * 7 * 100  # % em 7 dias
                pool_data['il_30d'] = abs(il_factor) * 30 * 100  # % em 30 dias
                
                # Score inicial (será refinado pelo analyzer)
                pool_data['score'] = self._calculate_initial_score(pool_data)
                
                # Timestamps
                pool_data['last_updated'] = datetime.utcnow().isoformat()
                pool_data['created_at'] = datetime.utcnow().isoformat()
                
                analyzed.append(pool_data)
                
            except Exception as e:
                logger.warning(f"Erro ao calcular métricas para pool {pool['id']}: {str(e)}")
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
