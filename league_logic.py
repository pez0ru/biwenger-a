import json
import logging as logger
import os
from functools import lru_cache
from operator import itemgetter
from typing import List, Dict, Union

import requests as requests
from pandas import DataFrame

url_api = 'https://biwenger.as.com/api/v2'
url_login = 'https://biwenger.as.com/api/v2/auth/login'
url_account = 'https://biwenger.as.com/api/v2/account'
url_players_market = 'https://biwenger.as.com/api/v2/user?fields=players(id,owner),market(*,-userID),-trophies'
url_players_league = 'https://biwenger.as.com/api/v2/players/la-liga/'
url_retire_market = "https://biwenger.as.com/api/v2/market?player="
url_add_player_market = "https://biwenger.as.com/api/v2/market"
url_all_players = "https://biwenger.as.com/api/v2/competitions/la-liga/data?lang=es&score=5"
url_ranking = "https://biwenger.as.com/api/v2/rounds/league"
url_league = 'https://biwenger.as.com/api/v2/league?include=all&fields=*,standings,tournaments,group,settings(description)'

class BiwengerApi:

    def __init__(self, user, password):
        self.user = user
        self.password = password
        self.token = self._get_token()
        self.auth = 'Bearer ' + self.token

    def _get_token(self):
        logger.info("Login process")
        data = {"email": self.user, "password": self.password}
        headers = {'Content-type': 'application/json', 'Accept': 'application/json, text/plain, */*'}
        contents = requests.post(url_login, data=json.dumps(data), headers=headers).json()
        logger.info("contents: " + repr(contents))
        if "token" in contents:
            logger.info("call login ok!")
            return contents['token']
        else:
            logger.info("error in login call, status: " + contents['status'])
            return "error, status" + contents['status']

    @lru_cache(1)
    def get_account_info(self):
        headers = {'Content-type': 'application/json', 'Accept': 'application/json, text/plain, */*', 'X-Lang': 'es',
                   'Authorization': self.auth}
        result = requests.get(url_account, headers=headers).json()
        if result['status'] == 200:
            logger.info("call login ok!")
        league_info = [x for x in result['data']['leagues'] if x['name'] == os.getenv("BIWENGER_LEAGUE_NAME")][0]
        id_league = league_info['id']
        id_user = league_info['user']['id']
        headers_league = {'Content-type': 'application/json', 'Accept': 'application/json, text/plain, */*',
                          'X-Lang': 'es',
                          'X-League': repr(id_league), 'X-User': repr(id_user), 'Authorization': self.auth}
        if result['status'] == 200:
            logger.info("call login ok!")
            return result, headers_league
        
    def get_league_info(self):
        """
        Get current status of the league
        """
        account_info, headers = self.get_account_info()
        league_info = requests.get(url_league, headers=headers).json()

        return league_info

    def get_players_in_market(self, free: bool) -> list:
        """
        Get an update of the current players in market
        Returns a list of players (dicts) with some useful information
        :return:{'date': 1658207321, 'until': 1658293200, 'price': 1930000, 'player': {'id': 555}, 'user': None,
        'id': 555, 'name': 'Asenjo', 'slug': 'sergio-asenjo', 'teamID': 418, 'position': 1, 'fantasyPrice': 13000000,
        'status': 'ok', 'priceIncrement': 140000, 'playedHome': 0, 'playedAway': 0, 'fitness': [None, None, None,
        None, None], 'points': 0, 'pointsHome': 0, 'pointsAway': 0, 'pointsLastSeason': 26}
        """

        full_market_info = []
        account_info, headers = self.get_account_info()
        result = requests.get(url_add_player_market, headers=headers).json()
        market_players = result['data']['sales']
        teams = self.get_teams_in_league()
        if free:
            market_players = [p for p in market_players if p['user'] is None]
        else:
            market_players = [p for p in market_players if p['user'] is not None]
        all_players = self.get_all_players_in_league()
        for offer in market_players:
            p = offer['player']['id']
            try:
                player = all_players[str(p)]
                offer.update(player)
                offer.update(self.get_player_extended_information(str(p)))
                offer.update({'team': teams[str(offer['teamID'])]})
            except NameError:
                # Sometimes we can't match players with it's id... working on solutions
                logger.warning(f'Player {p} not found')
            if self._is_high_cost_player(p):
                offer.update({"is_high_cost": self._is_high_cost_player(p)})
            full_market_info.append(offer)

        return [f for f in full_market_info if len(f) > 5]

    def _is_high_cost_player(self, player_id) -> bool:
        """
        If player is in top 20 by price we show a custom message in market notice
        :param player_id:
        :return:
        """
        all_players = self.get_all_players_in_league()
        top_n_players = sorted(list(all_players.values()), key=itemgetter('price'), reverse=True)[:20]
        for p in top_n_players:
            if int(player_id) == p['id']:
                return True
        return False

    def _is_top_player(self, player_id) -> bool:
        """
        If player is in top 20 by points we show a custom message in market notice
        :param player_id:
        :return:
        """
        all_players = self.get_all_players_in_league()
        top_n_players = sorted(list(all_players.values()), key=itemgetter('points'), reverse=True)[:20]
        for p in top_n_players:
            if int(player_id) == p['id']:
                return True
        return False

    def get_all_players_in_league(self):
        _, headers = self.get_account_info()
        req = requests.get(url_all_players, headers=headers).text
        all_players = json.loads(req)['data']['players']
        return all_players

    def get_teams_in_league(self):
        _, headers = self.get_account_info()
        teams = requests.get(url_all_players, headers=headers).json()['data']['teams']
        return {k: v['name'] for k, v in teams.items()}

    def get_next_round_time(self) -> Union[str, dict]:
        """
        Get exact time till next round. Get the url of possible line-ups for the last day before round.
        :return:
        """
        _, headers = self.get_account_info()
        req = requests.get(url_all_players, headers=headers).text
        data = json.loads(req)['data']
        events = data['events']
        rounds = data['season']['rounds']
        if 'active' in [r['status'] for r in rounds]:
            return "active"
        else:
            next_round = [r for r in rounds if r['id'] == events[0]['round']['id']][0]
            next_round.update({'date': events[0]['date']})
            if 'blogLineup' in data['social']:
                next_round.update({'blog': data['social']['blogLineup']})
            return next_round

    def get_last_user_transfers(self, enrich=False) -> List[Dict]:
        """
        Get last movements done in the league. Includes sales, purchases and clauses.
        :return:
        """
        _, headers = self.get_account_info()
        league_id = _['data']['leagues'][0]['id']
        url_transfers = f"https://biwenger.as.com/api/v2/league/{league_id}/board?type=transfer,market"

        transfers = requests.get(url_transfers, headers=headers).text
        movs = []
        all_players = self.get_all_players_in_league()
        for day in json.loads(transfers)['data']:
            content = day["content"]
            for mov in content:
                try:
                    info_player = all_players[str(mov["player"])]
                    mov_type = mov['type'] if 'type' in mov else 'transfer'
                    mov.update(info_player)
                    mov.update({'mov_type': mov_type})
                except:
                    print(f'Player {mov["player"]} not found')
            content = list(filter(lambda x: len(x) > 4, content))
            movs.append({'date': day['date'], 'content': content})
        return movs
    
    def get_league_balances(self):
        """
        """
        _, headers = self.get_account_info()
        league_id = _['data']['leagues'][0]['id']
        url_transfers = f"https://biwenger.as.com/api/v2/league/{league_id}/board?type=transfer,market"
        transfers = requests.get(url_transfers, headers=headers).text

        fichajes_clean = []

        for event in json.loads(transfers)['data']:
            # comprobamos si es movimiento de mercado o bonos por puntos
            if event['type'] in ['transfer','market','loan']:
                # guardamos fecha del evento
                fecha = event['date']

                # guardamos movimientos del dia
                for f in event['content']:
                    to = f.get('to',{'name':'Mercado'})['name']
                    fr = f.get('from',{'name':'Mercado'})['name']
                    amount = f['amount']

                    fich = {'fecha':fecha, 'de':fr, 'a':to, 'suma':amount}
                    fichajes_clean.append(fich)

            elif event['type'] == 'roundFinished':
                fecha = event['date']

                for user in event['content']['results']:
                    to = user['user']['name']
                    fr = 'Abono puntos'
                    amount = user.get('bonus',0)

                    fich = {'fecha':fecha, 'de':fr, 'a':to, 'suma':amount}
                    fichajes_clean.append(fich)
        
        if len(fichajes_clean)>0:
            df_fich = DataFrame(fichajes_clean).sort_values(by='fecha',ascending=False)
        else:
            df_fich = DataFrame(columns=['fecha','de','a','suma'])

        # Get users
        inf = self.get_league_info()
        users = []

        for u in inf['data']['standings']:
            users.append(u['name'])

        # update balances
        movs = {
            'ingresos':df_fich.loc[df_fich.de.isin(users)].groupby('de')['suma'].sum(),
            'gastos':df_fich.loc[(df_fich.de!='Abono puntos')&(df_fich.a.isin(users))].groupby('a')['suma'].sum(),
            'abonos':df_fich.loc[df_fich.de=='Abono puntos'].groupby('a')['suma'].sum()
        }

        saldos = DataFrame(movs)

        info_us = DataFrame(inf['data']['standings']).loc[:,['name','points','teamValue','teamSize']]
        info_us.index = info_us.name
        info_us.drop('name',axis=1,inplace=True)

        saldos = saldos.merge(info_us,left_index=True,right_index=True, how='right').fillna(0)

        saldos['saldo'] = 20000000 + saldos.ingresos + saldos.abonos - saldos.gastos 
        saldos['pujaMax'] = saldos.saldo + saldos.teamValue/4

        return saldos

    @staticmethod
    def raw_stats_analysis(stats: dict):
        """
        Percentage of minutes played by each player | n of matches in bench
        :param stats:
        :return:
        """
        absolute_minutes = len(stats) * 90
        round_not_played = [z for z in stats if 'rawStats' not in z]
        if round_not_played:
            for r in round_not_played:
                r['rawStats'] = {'minutesPlayed': 0}
        try:
            total_minutes_played = sum(
                [p['rawStats']['minutesPlayed'] for p in stats if p['match']['status'] == 'finished'])
        except:
            match_no_minutes_informed = [p for p in stats if 'minutesPlayed' not in p['rawStats']]
            for m in match_no_minutes_informed:
                m['rawStats']['minutesPlayed'] = 0
        matches_not_played = len([benchs for benchs in [mins['minutesPlayed']
                                                        for mins in [z['rawStats']
                                                                     for z in stats if z['match']['status'] ==
                                                                     'finished']] if benchs == 0])
        try:
            per_min_played = "{:.2f}".format(total_minutes_played / absolute_minutes)
        except:
            per_min_played = 0

        return {'per_min_played': per_min_played, 'matches_bench': matches_not_played}

    def get_player_extended_information(self, id_player: str):
        """
        Get advanced statistics from each player.
        Custom url is used passing player slug to resolve it
        :param id_player:
        :return:
        """
        url_player_info = f"https://biwenger.as.com/api/v2/players/la-liga/{id_player}?" \
                          f"https://cf.biwenger.com/api/v2/players/la-liga/{id_player}?" \
                          "lang=es&fields=*,team,fitness,reports(points,home,events,status(status,statusInfo)," \
                          "match(*,round,home,away),star),prices,competition,seasons,news,threads&callback=jsonp_1505664437"
        _, headers = self.get_account_info()
        info = requests.get(url_player_info, headers=headers).text
        info_format = json.loads(info)['data']
        sofascore_url = info_format['partner']['2']["url"]
        canonical_url = info_format['canonicalURL']
        raw_stats = self.raw_stats_analysis(info_format['reports'])
        url = sofascore_url if sofascore_url != 'https://www.sofascore.com' else canonical_url
        last_5_prices = [price[1] for price in info_format['prices'][-5:]]
        last_season = [s for s in info_format['seasons'] if s['id'] == '2022' and s['name'] ==
                       'Temporada 2021/2022']
        if not last_season:
            last_season = {'games': 0, 'points': '0'}
        else:
            last_season = last_season[0]
        if 'competition' in last_season:  # bug in the game, avoid segunda división players appear in stats
            last_season['games'] = 0
        matches_last_season = last_season['games'] if 'games' in last_season \
                                                      and isinstance(last_season['games'], int) else 0
        points_last_season = last_season['points'] if 'points' in last_season \
                                                      and isinstance(last_season['points'], str) else '0'
        try:
            real_avg_points = float(points_last_season) / float(matches_last_season)
        except:
            real_avg_points = 0.0
        try:
            price_variance = ((last_5_prices[4] - last_5_prices[0]) / last_5_prices[0]) * 100
        except:
            price_variance = 0.0
        avg_points_total = float(points_last_season) / 34
        extended_info = {"url": url,
                         "price_increment": "{:.2f}".format(price_variance),
                         "avg_points_per_match": "{:.2f}".format(real_avg_points),
                         "avg_total_points": "{:.2f}".format(avg_points_total),
                         "total_points_last": str(points_last_season),
                         "matches_played_last": matches_last_season}
        extended_info.update(raw_stats)
        return extended_info
