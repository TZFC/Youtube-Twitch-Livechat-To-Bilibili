# Livechat Bridge (YouTube & Twitch to Bilibili)

## Getting Started / 快速启动

### English
1. Double-click `run.bat` to launch the application.
2. The first launch will automatically set up the portable Python environment and download required dependencies. Please be patient.
3. Once started, open your browser and navigate to `http://127.0.0.1:8000`.

### 中文
1. 双击 `run.bat` 启动程序。
2. 首次运行会自动配置便携式 Python 环境并下载所需依赖库，请耐心等待。
3. 启动后，在浏览器中打开 `http://127.0.0.1:8000` 即可进入控制面板。

---

## Bilibili Authentication / B站登录凭据配置

### Method 1 (Recommended): In-App QR Code Login / 方法一（推荐）：扫码一键登录

- **English**:
  1. Open the dashboard at `http://127.0.0.1:8000`.
  2. Under **Bilibili Settings & Authentication**, click **"📱 Scan QR Code to Login"**.
  3. Open the Bilibili mobile app on your phone, scan the QR code displayed on screen, and tap confirm.
  4. The tool will automatically capture and save all required credentials (`SESSDATA`, `BILI_JCT`, `DedeUserID`, `BUVID3`, `BUVID4`, and `AC_TIME_VALUE`).
  - *Why this is best*: Scanning QR code generates a dedicated, independent session for this app. Browsing Bilibili on your PC will never invalidate this login, and the app will automatically renew its own session every time it launches.

- **中文**:
  1. 打开控制面板网页 `http://127.0.0.1:8000`。
  2. 在 **Bilibili Settings & Authentication** 区域，点击 **"📱 Scan QR Code to Login"**。
  3. 打开手机哔哩哔哩 App，使用右上角扫一扫扫描屏幕上的二维码并确认登录。
  4. 程序会自动获取并保存所有必要凭据（`SESSDATA`、`BILI_JCT`、`DedeUserID`、`BUVID3`、`BUVID4` 以及 `AC_TIME_VALUE`）。
  - *为什么推荐*：扫码登录会为本程序生成专属的独立会话凭据。你在电脑日常浏览或刷新B站不会影响该凭据，且每次启动程序时会自动续期，无需反复扫码。

---

### Method 2: Manual Cookie Extraction via Incognito Window / 方法二：使用无痕窗口手动提取凭据

If you prefer to manually extract your cookies, **do not copy them from your everyday browser session**, as your daily browser will periodically refresh its tokens and revoke the copied cookies within 2–4 days. Instead, use an **Incognito / Private Browsing window**.

#### English Instructions:
1. **Open an Incognito / Private Window** in your browser.
2. Go to [https://www.bilibili.com](https://www.bilibili.com) and log in.
3. **Make sure you are on the homepage** (`https://www.bilibili.com`). Do not stay on subdomains like `passport.bilibili.com` or `space.bilibili.com`, or `localStorage` will not be accessible.
4. Press `F12` to open Developer Tools:
   - **Cookies**: Go to `Application` (or `Storage`) $\rightarrow$ `Cookies` $\rightarrow$ `https://www.bilibili.com`. Copy the values for:
     - `SESSDATA`
     - `BILI_JCT`
     - `BUVID3`
     - `DEDEUSERID`
   - **Refresh Token (`ac_time_value`)**: Go to the `Console` tab and enter:
     ```javascript
     window.localStorage.ac_time_value
     ```
     Copy the output string into the `AC_TIME_VALUE` field in the dashboard.
5. **CRITICAL STEP**: **Close the Incognito window directly WITHOUT clicking "Log Out" (退出登录)**.
   - *Why*: Clicking "Log Out" sends an API call that revokes the session on Bilibili's servers. Simply closing the window destroys the local browser cache while keeping the session alive for your app to own exclusively.
6. *Note on `ac_time_value`*: If `window.localStorage.ac_time_value` returns `undefined`, ensure the current page address is strictly `https://www.bilibili.com` and refresh once. Even if left blank, `SESSDATA` extracted from Incognito will remain valid for its standard ~30-day lifespan.

#### 中文操作指南：
如果需要手动复制 Cookie，**请勿直接从日常使用的普通浏览器窗口中复制**，因为日常浏览器在后台自动刷新 Token 时会使复制出来的 Cookie 在 2～4 天内失效。请严格按照以下步骤使用**无痕（隐私）窗口**提取：

1. **打开浏览器的无痕模式 / 隐身窗口（Incognito / InPrivate）**。
2. 访问 [https://www.bilibili.com](https://www.bilibili.com) 并完成登录。
3. **务必确保当前网页为主站首页**（`https://www.bilibili.com`）。切勿停留在 `passport.bilibili.com` 或 `space.bilibili.com` 等子域名，否则无法读取主站的 `localStorage`。
4. 按键盘 `F12` 打开开发者工具：
   - **Cookie 提取**：切换到 `Application`（应用程序 / 存储） $\rightarrow$ `Cookies` $\rightarrow$ `https://www.bilibili.com`，依次复制以下值粘贴到控制面板中：
     - `SESSDATA`
     - `BILI_JCT`
     - `BUVID3`
     - `DEDEUSERID`
   - **刷新凭据（`ac_time_value`）提取**：切换到 `Console`（控制台）标签页，输入以下命令并回车：
     ```javascript
     window.localStorage.ac_time_value
     ```
     将输出的字符串复制粘贴到控制面板的 `AC_TIME_VALUE` 输入框中。
5. **关键步骤**：**直接关闭无痕窗口，千万不要点击页面上的“退出登录”！**
   - *原因说明*：点击“退出登录”会主动向B站服务器发送注销请求，立即作废该会话；而直接关闭无痕窗口仅销毁浏览器本地记录，B站服务器端的会话依然完好，并由本程序独占持有与自动续期。
6. *关于找不到 `ac_time_value` 的说明*：如果控制台返回 `undefined`，请检查当前网页地址是否严格为 `https://www.bilibili.com` 并刷新一次网页。即使不填 `AC_TIME_VALUE`，只要是通过无痕窗口提取的 `SESSDATA`，也能正常稳定使用约 30 天的基础有效期。
