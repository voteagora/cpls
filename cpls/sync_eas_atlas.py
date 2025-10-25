import time, json

from collections import defaultdict

from .gcs import GCSClient
from .sync import Sync, SkipProposal, FIVE_MINUTES_IN_SECONDS

from .title_processor import get_title_from_proposal_description

from .config import ALCHEMY_API_KEY

class EASAtlasSync(Sync):

    SOURCE = 'eas-atlas'

    async def read_votes(self, proposal_id):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(f"""select * from atlas."OffChainVote" ocv where "proposalId" = '{proposal_id}';""")
            return rows

    async def refresh_list(self, gcs_client: 'GCSClient'):


        ######################################
        # Step 1 - Get a list of recent-ish proposals.  Think either the "full list of any proposal ever" OR "just stuff that may or may not be ready to archive"
        #

        # THIS IS TOTALLY FLAWED LOGIC, the point is to make a file format that can be consumed as a second source.
        known_create_attestations = {}
        easa = """0xc89066cf84cc86c3cbb9cc148dbf7514a1b897ad5dbaf878716f6beee89fd6ff
                0xe73cdaca33221711fddfe6c7302b0f1d1d9bf093a9d04643710890d74b865fec
                0xffeefe5d1263a0b1275e407c4c8ecbf87031d7bb2f25b5a9911b14eacee974bc
                0x46e273e2820a4254c6d3b79cf101d82dac8a25abb4eb0e8dd940c4758553fac0
                0xd70f9590ca82e2d263d95ba72a76c9bc9e2a6007c5c906a699794776a2f47d4d
                0x0701b609c2904f1b05bdaa38ab898ad74478c3a3eff4e0691a60509090ef2af2
                0x42247c00390396ad0d598e3ec39bc349c6a3f17fe44bcf574707a904960c127e""".split("\n")
        known_create_attestations['optimism'] = [e.strip() for e in easa]
        # End of flawed logic

        headers = {'alchemy-api-key': ALCHEMY_API_KEY}

        anything_changed = False
        for proposals_uid in known_create_attestations[self.infra_dao_slug]:

            for chain_id in [10, 1]:

                url = f"https://blacache-production.up.railway.app/decoded_eas/{chain_id}/attestation/{proposals_uid}"
                async with httpx.AsyncClient() as client:
                    response = await client.get(url, headers=headers)

                if response.status_code != 200:
                    print(f"Failed to fetch proposal {proposals_uid}")
                    continue

                proposal_attestation = response.json()

                if proposal_attestation['attestation']['uid'] == '0x0000000000000000000000000000000000000000000000000000000000000000':
                    print(f"Failed to fetch proposal {proposals_uid}")
                    continue

                proposal = proposal_attestation['attestation']
                proposal['chain_id'] = proposal_attestation['chain_id']
                proposal.update(proposal_attestation['decoded_data'])
                del proposal['data']
                proposal['resolver'] = proposal_attestation['schema']['resolver']

                proposal_type = proposal['proposal_type']

                proposal['id'] = str(proposal['id'])

                proposal_id = proposal['id']

                try:
                    existing_proposal_hash = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
                except SkipProposal as e:
                    print(e)
                    continue

                proposal['title'] = get_title_from_proposal_description(proposal['description'])
                proposal['proposer_ens'] = await self.bc.get_ens(proposal['proposer'])

                votes = await self.read_votes(proposal_id)

                

                if proposal_type in ('OPTIMISTIC', 'STANDARD'):

                    outcome = defaultdict(lambda: defaultdict(int))

                    for vote in votes:
                        support = vote['vote']
                        support = json.loads(support)
                        for elem in support:
                            outcome[vote['citizenCategory']][elem] += 1

                elif proposal_type == 'APPROVAL': 

                    outcome = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
                   
                    for vote in votes:
                        support = vote['vote']
                        options, supports =json.loads(support)
                        
                        for option, support in zip(options, supports):
                            outcome[vote['citizenCategory']][option][support] += 1

                proposal['outcome'] = outcome
            
                try:
                    proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
                except SkipProposal as e:
                    print(e)
                    continue


                # This section here, enriches the proposal object, in a way that will only update,
                # if the hash for the proposal's state changes. Downstream consumers can either use it, accepting
                # the caveate, or re-calculate it.

                start_block = proposal['start_block']
                start_blocktime = await self.get_timestamp(chain_id, start_block)
                proposal['start_blocktime'] = start_blocktime

                end_block = start_block + 259200 # TODO - the EAS Attestation needs the end-block timestamp.

                proposal['end_block'] = end_block
                end_blocktime = await self.get_timestamp(chain_id, end_block)
                proposal['end_blocktime'] = end_blocktime

                liveness = 'live'

                cur_time = int(time.time())                

                if cur_time > proposal['end_blocktime'] + FIVE_MINUTES_IN_SECONDS:
                    liveness = 'archived'

                anything_changed = True

                await self.overwrite_proposal(proposal, proposal_hash, liveness, gcs_client)

            if anything_changed:
                await self.refresh_source_list(gcs_client)
                await self.refresh_full_list(gcs_client)

