# Security Policy

## Clickjacking Vulnerability Fix

### Issue
The application was vulnerable to clickjacking attacks due to missing `X-Frame-Options` header. Clickjacking (UI redress attack) is a malicious technique where attackers trick users into clicking on something different from what they perceive, potentially revealing confidential information or taking control of their interaction.

### Solution
We have implemented the following security headers in our nginx configuration to prevent clickjacking attacks:

#### Primary Fix: X-Frame-Options Header
```nginx
add_header X-Frame-Options "DENY" always;
```

**Options available:**
- `DENY`: Completely prevents the page from being displayed in a frame (recommended for most applications)
- `SAMEORIGIN`: Allows framing only if the frame and page have the same origin
- `ALLOW-FROM uri`: Allows framing from specific URI (deprecated, use CSP instead)

#### Modern Alternative: Content Security Policy (CSP)
```nginx
add_header Content-Security-Policy "frame-ancestors 'none';" always;
```

The CSP `frame-ancestors` directive is the modern replacement for `X-Frame-Options` and provides more granular control.

### Implementation

#### For Production Deployments
Use the main `nginx.conf` file which includes:
- HTTPS redirect
- SSL configuration
- Complete security headers suite
- Performance optimizations

```bash
# Copy the configuration to your nginx sites-enabled directory
sudo cp nginx.conf /etc/nginx/sites-available/bruin-cloud
sudo ln -s /etc/nginx/sites-available/bruin-cloud /etc/nginx/sites-enabled/
sudo nginx -t  # Test configuration
sudo systemctl reload nginx
```

#### For Docker/Container Deployments
Use the `docker/nginx.conf` file which includes:
- Container-optimized configuration
- Backend proxy setup
- Security headers for all endpoints

```dockerfile
# In your Dockerfile
COPY docker/nginx.conf /etc/nginx/conf.d/default.conf
```

#### For Kubernetes Deployments
Create a ConfigMap with the nginx configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: nginx-config
data:
  nginx.conf: |
    # Include content from docker/nginx.conf
```

### Verification

After deploying the fix, verify the headers are present:

```bash
# Check for X-Frame-Options header
curl -I https://your-domain.com | grep -i x-frame-options

# Expected output:
# X-Frame-Options: DENY

# Check for CSP header
curl -I https://your-domain.com | grep -i content-security-policy

# Expected output should include:
# Content-Security-Policy: frame-ancestors 'none'; ...
```

### Additional Security Headers Implemented

Along with the clickjacking fix, we've implemented additional security headers:

- `X-Content-Type-Options: nosniff` - Prevents MIME type sniffing
- `X-XSS-Protection: 1; mode=block` - Enables XSS filtering
- `Referrer-Policy: strict-origin-when-cross-origin` - Controls referrer information
- `Strict-Transport-Security` - Enforces HTTPS (production config only)

### Testing

You can test the clickjacking protection using online tools:
- [Mozilla Observatory](https://observatory.mozilla.org/)
- [Security Headers](https://securityheaders.com/)

Or create a simple test page:

```html
<!DOCTYPE html>
<html>
<head>
    <title>Clickjacking Test</title>
</head>
<body>
    <h1>Clickjacking Protection Test</h1>
    <iframe src="https://your-domain.com" width="800" height="600"></iframe>
    <p>If the iframe above is blocked, the protection is working correctly.</p>
</body>
</html>
```

## Reporting Security Issues

Please report security issues to security@bruin-data.com
