"""Fresh isolated-volume startup proof; never deletes the main project's volumes/cache."""
from support import *

def main():
    project='flashcoupon-repro-'+str(int(time.time()))
    override=LOCAL/'fresh-start.yaml'
    services=('app','relay','worker','mysql','redis','proxy')
    # The first Compose file remains the root; all bind paths still resolve to the repo.
    override.write_text('services:\n'+''.join('  '+s+':\n    ports: !reset []\n'+('    image: flashcoupon-repro-proxy:local\n    pull_policy: never\n' if s=='proxy' else '') for s in services))
    run(['docker','image','tag','flashcoupon-proxy:latest','flashcoupon-repro-proxy:local'])
    cmd=['docker','compose','-p',project,'-f','compose.yaml','-f','compose.sharding.yaml','-f',str(override)]
    started=time.monotonic();created=False
    try:
        assert not run(['docker','ps','-aq','--filter','label=com.docker.compose.project='+project]).strip()
        created=True
        run(cmd+['up','-d','--no-build','app','relay','worker'])
        health={}
        for service in ('app','relay','worker'):
            def state():
                return run(['docker','inspect',project+'-'+service+'-1','--format','{{.State.Health.Status}}']).strip()=='healthy'
            until(state,180);run(cmd+['exec','-T',service,'/flashcoupon','--healthcheck']);health[service]='healthy'
        output=run(cmd+['exec','-T','mysql','mysql','-uroot','-pflashcoupon','-N','-e',"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('coupon_0','coupon_1') AND table_name IN ('coupons_0','coupons_1');"])
        assert int(output.strip())==4
        save('fresh-start.json',dict(passed=True,fresh_data_volumes=True,user_coupon_tables=4,roles=health,seconds=round(time.monotonic()-started,3),main_data_untouched=True))
        print('Fresh-volume bootstrap passed',health,flush=True)
    finally:
        if created:
            run(cmd+['down']) # Deliberately NOT down -v: toolbox cache names are shared.
            volumes=[project+'_'+name for name in ('mysql-data','redis-data','mq-data')]
            assert all(v.startswith(project+'_') for v in volumes)
            run(['docker','volume','rm',*volumes])

if __name__=='__main__':main()
