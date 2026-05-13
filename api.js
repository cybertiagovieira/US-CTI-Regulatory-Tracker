export async function fetchSha(repo) {
    if(!repo) return '';
    try {
        const res = await fetch(`https://api.github.com/repos/${repo}/contents/master_data.json`);
        if(res.ok) {
            const data = await res.json();
            return data.sha;
        }
    } catch(e) { console.error(e); }
    return '';
}

export async function githubFetch(url, options = {}, retries = 3) {
    for (let i = 0; i < retries; i++) {
        const response = await fetch(url, options);
        if (response.ok) return response;
        if (response.status === 403 || response.status === 429) {
            const retryAfter = response.headers.get('retry-after');
            const rateLimitRemaining = response.headers.get('x-ratelimit-remaining');
            if (retryAfter || rateLimitRemaining === '0') {
                let waitTime = 2000;
                if (retryAfter) { waitTime = parseInt(retryAfter) * 1000; } 
                else {
                    const resetTime = response.headers.get('x-ratelimit-reset');
                    if (resetTime) {
                        const resetDate = new Date(parseInt(resetTime) * 1000);
                        waitTime = Math.max(resetDate - new Date(), 1000);
                        if (waitTime > 60000) throw new Error(`API Quota Exhausted. Ban lifts at: ${resetDate.toLocaleTimeString()}`);
                    }
                }
                console.warn(`[Rate Limit] Imposed penalty. Pausing thread for ${waitTime}ms...`);
                await new Promise(resolve => setTimeout(resolve, waitTime));
                continue; 
            }
        }
        return response; 
    }
    throw new Error("API Failure: Max retry attempts exceeded.");
}

export function b64EncodeUnicode(str) {
    return btoa(encodeURIComponent(str).replace(/%([0-9A-F]{2})/g, function(match, p1) {
        return String.fromCharCode('0x' + p1);
    }));
}

export async function syncToGitHubAtomic(action, target, token, repo) {
    if(!token) return alert("GitHub PAT Required.") && false;
    const url = `https://api.github.com/repos/${repo}/contents/master_data.json`;
    try {
        const remoteRes = await githubFetch(`${url}?t=${new Date().getTime()}`, { headers: { 'Authorization': `token ${token}` } });
        if (!remoteRes.ok) throw new Error("Could not fetch remote state.");
        const remoteData = await remoteRes.json();
        const remoteText = decodeURIComponent(escape(atob(remoteData.content)));
        let remoteJson = JSON.parse(remoteText);
        
        if (action === 'ADD') { remoteJson.push(target); } 
        else if (action === 'DELETE') { remoteJson = remoteJson.filter(d => d.id !== target); }
        
        const content = b64EncodeUnicode(JSON.stringify(remoteJson, null, 2));
        const body = { message: `Dashboard Admin: ${action} Operation`, content: content, branch: "main", sha: remoteData.sha };
        const pushRes = await githubFetch(url, { method: 'PUT', headers: { 'Authorization': `token ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        
        if (pushRes.ok) {
            const pushData = await pushRes.json();
            return { success: true, sha: pushData.content.sha, newData: remoteJson };
        } else {
            alert("API Rejection: File synchronization failed due to deep conflict.");
            return { success: false };
        }
    } catch(e) {
        console.error(e);
        alert(e.message || "Network routing or synchronization error.");
        return { success: false };
    }
}