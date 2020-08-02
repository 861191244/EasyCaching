namespace EasyCaching.Redis
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using EasyCaching.Core;
    using EasyCaching.Core.Internal;
    using StackExchange.Redis;

    /// <summary>
    /// Default redis caching provider.
    /// </summary>
    public partial class DefaultRedisCachingProvider : IRedisCachingProvider
    {
        public bool HMSet(string cacheKey, Dictionary<string, object> vals, TimeSpan? expiration = null)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            var list = new List<HashEntry>();

            foreach (var item in vals)
            {
                if (item.Value.GetType().Equals(typeof(byte[])))
                {
                    list.Add(new HashEntry(item.Key, (byte[])item.Value));
                }
                else
                {
                    list.Add(new HashEntry(item.Key, item.Value.ToString()));
                }
            }

            _cache.HashSet(cacheKey, list.ToArray());

            var flag = true;

            if (expiration.HasValue)
            {
                flag = _cache.KeyExpire(cacheKey, expiration);
            }

            return flag;
        }

        public bool HSet(string cacheKey, string field, object cacheValue)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullOrWhiteSpace(field, nameof(field));

            var list = new List<HashEntry>();

            if (cacheValue.GetType().Equals(typeof(byte[])))
            {
                list.Add(new HashEntry(field, (byte[])cacheValue));
            }
            else
            {
                list.Add(new HashEntry(field, cacheValue.ToString()));
            }

            _cache.HashSet(cacheKey, list.ToArray());

            return true;
        }

        public bool HExists(string cacheKey, string field)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullOrWhiteSpace(field, nameof(field));

            return _cache.HashExists(cacheKey, field);
        }

        public long HDel(string cacheKey, IList<string> fields = null)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            if (fields != null && fields.Any())
            {
                return _cache.HashDelete(cacheKey, fields.Select(x => (RedisValue)x).ToArray());
            }
            else
            {
                var flag = _cache.KeyDelete(cacheKey);
                return flag ? 1 : 0;
            }
        }

        public object HGet(string cacheKey, string field)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullOrWhiteSpace(field, nameof(field));

            var res = _cache.HashGet(cacheKey, field);
            return res;
        }

        public Dictionary<string, object> HGetAll(string cacheKey)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            var dict = new Dictionary<string, object>();

            var vals = _cache.HashGetAll(cacheKey);

            foreach (var item in vals)
            {
                if (!dict.ContainsKey(item.Name)) dict.Add(item.Name, item.Value);
            }

            return dict;
        }

        public long HIncrBy(string cacheKey, string field, long val = 1)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullOrWhiteSpace(field, nameof(field));

            return _cache.HashIncrement(cacheKey, field, val);
        }

        public List<string> HKeys(string cacheKey)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            var keys = _cache.HashKeys(cacheKey);
            return keys.Select(x => x.ToString()).ToList();
        }

        public long HLen(string cacheKey)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            return _cache.HashLength(cacheKey);
        }

        public List<object> HVals(string cacheKey)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            return _cache.HashValues(cacheKey).Select(x => (object)x).ToList();
        }

        public Dictionary<string, object> HMGet(string cacheKey, IList<string> fields)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullAndCountGTZero(fields, nameof(fields));

            var dict = new Dictionary<string, object>();

            var list = _cache.HashGet(cacheKey, fields.Select(x => (RedisValue)x).ToArray());

            for (int i = 0; i < fields.Count(); i++)
            {
                if (!dict.ContainsKey(fields[i]))
                {
                    dict.Add(fields[i], list[i]);
                }
            }

            return dict;
        }

        public async Task<bool> HMSetAsync(string cacheKey, Dictionary<string, object> vals, TimeSpan? expiration = null)
        {
            var list = new List<HashEntry>();

            foreach (var item in vals)
            {
                if (item.Value.GetType().Equals(typeof(byte[])))
                {
                    list.Add(new HashEntry(item.Key, (byte[])item.Value));
                }
                else
                {
                    list.Add(new HashEntry(item.Key, item.Value.ToString()));
                }
            }

            await _cache.HashSetAsync(cacheKey, list.ToArray());

            var flag = true;


            if (expiration.HasValue)
            {
                flag = await _cache.KeyExpireAsync(cacheKey, expiration.Value);
            }

            return flag;
        }

        public async Task<bool> HSetAsync(string cacheKey, string field, object cacheValue)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullOrWhiteSpace(field, nameof(field));

            var list = new List<HashEntry>();

            if (cacheValue.GetType().Equals(typeof(byte[])))
            {
                list.Add(new HashEntry(field, (byte[])cacheValue));
            }
            else
            {
                list.Add(new HashEntry(field, cacheValue.ToString()));
            }

            await _cache.HashSetAsync(cacheKey, list.ToArray());

            return true;
        }

        public async Task<bool> HExistsAsync(string cacheKey, string field)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullOrWhiteSpace(field, nameof(field));

            return await _cache.HashExistsAsync(cacheKey, field);
        }

        public async Task<long> HDelAsync(string cacheKey, IList<string> fields)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            if (fields != null && fields.Any())
            {
                return await _cache.HashDeleteAsync(cacheKey, fields.Select(x => (RedisValue)x).ToArray());
            }
            else
            {
                var flag = await _cache.KeyDeleteAsync(cacheKey);
                return flag ? 1 : 0;
            }
        }

        public async Task<object> HGetAsync(string cacheKey, string field)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullOrWhiteSpace(field, nameof(field));

            var res = await _cache.HashGetAsync(cacheKey, field);
            return res;
        }

        public async Task<Dictionary<string, object>> HGetAllAsync(string cacheKey)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            var dict = new Dictionary<string, object>();

            var vals = await _cache.HashGetAllAsync(cacheKey);

            foreach (var item in vals)
            {
                if (!dict.ContainsKey(item.Name)) dict.Add(item.Name, item.Value);
            }

            return dict;
        }

        public async Task<long> HIncrByAsync(string cacheKey, string field, long val = 1)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullOrWhiteSpace(field, nameof(field));

            return await _cache.HashIncrementAsync(cacheKey, field, val);
        }

        public async Task<List<string>> HKeysAsync(string cacheKey)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            var keys = await _cache.HashKeysAsync(cacheKey);
            return keys.Select(x => x.ToString()).ToList();
        }

        public async Task<long> HLenAsync(string cacheKey)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            return await _cache.HashLengthAsync(cacheKey);
        }

        public async Task<List<object>> HValsAsync(string cacheKey)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));

            return (await _cache.HashValuesAsync(cacheKey)).Select(x => (object)x).ToList();
        }

        public async Task<Dictionary<string, object>> HMGetAsync(string cacheKey, IList<string> fields)
        {
            ArgumentCheck.NotNullOrWhiteSpace(cacheKey, nameof(cacheKey));
            ArgumentCheck.NotNullAndCountGTZero(fields, nameof(fields));

            var dict = new Dictionary<string, object>();

            var res = await _cache.HashGetAsync(cacheKey, fields.Select(x => (RedisValue)x).ToArray());

            for (int i = 0; i < fields.Count(); i++)
            {
                if (!dict.ContainsKey(fields[i]))
                {
                    dict.Add(fields[i], res[i]);
                }
            }

            return dict;
        }
    }
}
