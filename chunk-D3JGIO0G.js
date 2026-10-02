import{$n as uE,An as ou,Bt as dp,Cn as nD,E as HE,En as np,Et as _,F as Jf,Ft as bI,G as Op,H as NI,In as qE,Jt as fl,L as Kg,Ln as qI,M as IL,Mn as pe,Mt as ap,N as Il,Nt as ay,Ot as _o$1,P as JE,Pt as b,Qt as gc,R as Kh,Rn as qh,Rt as cp,Tn as no$1,Tt as Zy,Ut as eD,V as NE,Vt as dr$1,Wn as rp,Wt as eE,X as QE,Xt as gE,Yn as tD,Z as QI,Zt as gL,_ as Ep,_n as mL,_r as zr$1,a as CI,an as ht$1,at as Sp,b as GI,bn as mm,bt as Yf,c as Ch,cr as xE,ct as Uh,d as DL,dn as kg,en as gp,er as up,fr as yL,gt as Wn,h as Ec,hr as zI,i as C,in as hp,ir as wL,it as So$1,j as II,kn as op,kt as aE,lt as Up,m as Dp,mn as lE,nr as vL,o as CL,on as hu,pr as yc,pt as W,qn as sD,rn as hi,rt as Rp,s as Ce,sn as ip,tn as hE,u as DI,un as ji$1,ut as Vi$1,v as Fe,vn as ma$1,w as Gl,wn as ni,xn as mr$1,y as G$1,yn as mc,zn as ql}from"./chunk-BOjKePw7.js";import{A as hc,B as od,C as Xl,D as cd,E as ad,F as jl,G as ud,H as rd,I as jo$1,J as xe,K as vi,L as jt,M as j,N as jd,O as ed,P as ji$2,Q as zl,S as Wo$1,T as Zl,U as sd,V as q,W as td,X as yi,Y as yd,Z as ze,_ as Ui$1,a as Ce$1,b as Vs$1,c as Gl$1,d as Jl,f as Kl,g as Ql,h as Ne,j as id,k as fc,l as Ho$1,m as La$1,n as $i$1,o as Dd,p as Ko$1,q as x,r as $s$1,s as Ei,u as It,w as Yl,x as Wl,y as Vl,z as nd}from"./main-TMDJT5KQ.js";function Le(...n){let i=[];for(let e=0;e<n.length;e++){let t=n[e];if(!t)continue;let o=typeof t;if(o===`string`||o===`number`)i.push(t);else if(o===`object`){let r=Array.isArray(t)?[Le(...t)]:Object.entries(t).map(([s,a])=>a?s:void 0);i=r.length?i.concat(r.filter(s=>!!s)):i}}return i.join(` `).trim()}var Xo=Object.defineProperty;var Mi=Object.getOwnPropertySymbols;var Jo=Object.prototype.hasOwnProperty;var Yo=Object.prototype.propertyIsEnumerable;var Ii=(n,i,e)=>i in n?Xo(n,i,{enumerable:!0,configurable:!0,writable:!0,value:e}):n[i]=e;var Li=(n,i)=>{for(var e in i||(i={}))Jo.call(i,e)&&Ii(n,e,i[e]);if(Mi)for(var e of Mi(i))Yo.call(i,e)&&Ii(n,e,i[e]);return n};function $i(...n){let i=[];for(let e=0;e<n.length;e++){let t=n[e];if(!t)continue;let o=typeof t;if(o===`string`||o===`number`)i.push(t);else if(o===`object`){let r=Array.isArray(t)?[$i(...t)]:Object.entries(t).map(([s,a])=>a?s:void 0);i=r.length?i.concat(r.filter(s=>!!s)):i}}return i.join(` `).trim()}function er(n){return typeof n==`function`&&`call`in n&&`apply`in n}function tr({skipUndefined:n=!1},...i){return i?.reduce((e,t={})=>{for(let o in t){let r=t[o];if(!(n&&r===void 0))if(o===`style`)e.style=Li(Li({},e.style),t.style);else if(o===`class`||o===`className`)e[o]=$i(e[o],t[o]);else if(er(r)){let s=e[o];e[o]=s?(...a)=>{s(...a),r(...a)}:r}else e[o]=r}return e},{})}function cn(...n){return tr({skipUndefined:!1},...n)}var Ht={};function Qe(n=`pui_id_`){return Object.hasOwn(Ht,n)||(Ht[n]=0),Ht[n]++,`${n}${Ht[n]}`}var Ai=(()=>{class n extends $i$1{name=`common`;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac,providedIn:`root`})}return n})();var X=new _(`PARENT_INSTANCE`);var G=(()=>{class n{document=C(Wn);platformId=C(kg);el=C(dr$1);injector=C(Fe);cd=C(DL);renderer=C(ma$1);config=C(La$1);$parentInstance=C(X,{optional:!0,skipSelf:!0})??void 0;baseComponentStyle=C(Ai);baseStyle=C($i$1);scopedStyleEl;parent=this.$params.parent;cn=Le;_themeScopedListener;themeChangeListenerMap=new Map;dt=mL();unstyled=mL();pt=mL();ptOptions=mL();$attrSelector=Qe(`pc`);get $name(){return this.componentName||`UnknownComponent`}get $hostName(){let e=this.hostName;return So$1(e)?e():e}get $el(){return this.el?.nativeElement}directivePT=_o$1(void 0);directiveUnstyled=_o$1(void 0);$unstyled=sD(()=>this.unstyled()??this.directiveUnstyled()??this.config?.unstyled()??!1);$pt=sD(()=>q(this.pt()||this.directivePT(),this.$params));get $globalPT(){return this._getPT(this.config?.pt(),void 0,e=>q(e,this.$params))}get $defaultPT(){return this._getPT(this.config?.pt(),void 0,e=>this._getOptionValue(e,this.$hostName||this.$name,this.$params)||q(e,this.$params))}_$styleCache;get $style(){return this._$styleCache||(this._$styleCache=G$1(G$1({theme:void 0,css:void 0,classes:void 0,inlineStyles:void 0},(this._getHostInstance(this)||{}).$style),this._componentStyle)),this._$styleCache}get $styleOptions(){return{nonce:this.config?.csp().nonce}}_$paramsCache;get $params(){if(!this._$paramsCache){let e=this._getHostInstance(this)||this.$parentInstance;this._$paramsCache={instance:this,parent:{instance:e}}}return this._$paramsCache}onInit(){}onChanges(e){}onDoCheck(){}onAfterContentInit(){}onAfterContentChecked(){}onAfterViewInit(){}onAfterViewChecked(){}onDestroy(){}constructor(){this._shareStylesWithShadowRoot(),hu(e=>{this.document&&!hc(this.platformId)&&(this.dt()?(this._loadScopedThemeStyles(this.dt()),this._themeScopedListener=()=>this._loadScopedThemeStyles(this.dt()),this._themeChangeListener(`_themeScopedListener`,this._themeScopedListener)):this._unloadScopedThemeStyles()),e(()=>{this._offThemeChangeListener(`_themeScopedListener`)})}),hu(e=>{this.document&&!hc(this.platformId)&&(this.$unstyled()||(this._loadCoreStyles(),this._themeChangeListener(`_loadCoreStyles`,this._loadCoreStyles))),e(()=>{this._offThemeChangeListener(`_loadCoreStyles`)})}),this._hook(`onBeforeInit`)}ngOnInit(){this._$paramsCache=void 0,this._$styleCache=void 0,this._loadCoreStyles(),this._loadStyles(),this.onInit(),this._hook(`onInit`)}ngOnChanges(e){this.onChanges(e),this._hook(`onChanges`,e)}ngDoCheck(){this.onDoCheck(),this._hook(`onDoCheck`)}ngAfterContentInit(){this.onAfterContentInit(),this._hook(`onAfterContentInit`)}ngAfterContentChecked(){this.onAfterContentChecked(),this._hook(`onAfterContentChecked`)}ngAfterViewInit(){this.$el?.setAttribute(this.$attrSelector,``),this.config?.verified()===!1&&ji$2(),this.onAfterViewInit(),this._hook(`onAfterViewInit`)}ngAfterViewChecked(){this.onAfterViewChecked(),this._hook(`onAfterViewChecked`)}ngOnDestroy(){this._removeThemeListeners(),this._unloadScopedThemeStyles(),this.onDestroy(),this._hook(`onDestroy`)}_mergeProps(e,...t){return Ei(e)?e(...t):cn(...t)}_getHostInstance(e){return e?this.$hostName?this.$name===this.$hostName?e:this._getHostInstance(e.$parentInstance):e.$parentInstance:void 0}_getPropValue(e){return this[e]||this._getHostInstance(this)?.[e]}_getOptionValue(e,t=``,o={}){return vi(e,t,o)}_hook(e,...t){if(this.$hostName||!this.pt()&&!this.directivePT()&&!this.config?.pt())return;let o=this._usePT(this._getPT(this.$pt(),this.$name),this._getOptionValue,`hooks.${e}`),r=this._useDefaultPT(this._getOptionValue,`hooks.${e}`);o?.(...t),r?.(...t)}_load(){jd.isStyleNameLoaded(`base`)||(this.baseStyle.loadBaseCSS(this.$styleOptions),this._loadGlobalStyles(),jd.setLoadedStyleName(`base`)),this._loadThemeStyles()}_loadStyles(){this._load(),this._themeChangeListener(`_load`,()=>this._load())}_shareStylesWithShadowRoot(){if(hc(this.platformId))return;let e=this.$el?.getRootNode?.();typeof ShadowRoot>`u`||!(e instanceof ShadowRoot)||C(Ce).onDestroy(C(Ui$1).addShadowRoot(e))}_loadGlobalStyles(){let e=this._useGlobalPT(this._getOptionValue,`global.css`,this.$params);j(e)&&this.baseStyle.load(e,G$1({name:`global`},this.$styleOptions))}_loadCoreStyles(){!jd.isStyleNameLoaded(this.$style?.name)&&this.$style?.name&&(this.baseComponentStyle.loadCSS(this.$styleOptions),this.$style.loadCSS(this.$styleOptions),jd.setLoadedStyleName(this.$style.name))}_loadThemeStyles(){if(!(this.$unstyled()||this.config?.theme()===`none`)){if(!x.isStyleNameLoaded(`common`)){let{primitive:e,semantic:t,global:o,style:r}=this.$style?.getCommonTheme?.()||{};this.baseStyle.load(e?.css,G$1({name:`primitive-variables`,variables:!0},this.$styleOptions)),this.baseStyle.load(t?.css,G$1({name:`semantic-variables`,variables:!0},this.$styleOptions)),this.baseStyle.load(o?.css,G$1({name:`global-variables`,variables:!0},this.$styleOptions)),this.baseStyle.loadBaseStyle(G$1({name:`global-style`},this.$styleOptions),r),x.setLoadedStyleName(`common`)}if(!x.isStyleNameLoaded(this.$style?.name)&&this.$style?.name){let{css:e,style:t}=this.$style?.getComponentTheme?.()||{};this.$style?.load(e,G$1({name:`${this.$style?.name}-variables`,variables:!0},this.$styleOptions)),this.$style?.loadStyle(G$1({name:`${this.$style?.name}-style`},this.$styleOptions),t),x.setLoadedStyleName(this.$style?.name)}if(!x.isStyleNameLoaded(`layer-order`)){let e=this.$style?.getLayerOrderThemeCSS?.();this.baseStyle.load(e,G$1({name:`layer-order`,first:!0},this.$styleOptions)),x.setLoadedStyleName(`layer-order`)}}}_loadScopedThemeStyles(e){this.config?.theme()?.options?.cssVariables===!1&&this.$style?.name&&x.addScopedToken({[this.$style.name]:e})&&(x.deleteLoadedStyleName(this.$style.name),this._loadThemeStyles());let{css:t}=this.$style?.getPresetTheme?.(e,`[${this.$attrSelector}]`)||{},o=this.$style?.load(t,G$1({name:`${this.$attrSelector}-${this.$style?.name}`},this.$styleOptions));this.scopedStyleEl=o?.el}_unloadScopedThemeStyles(){this.baseStyle.useStyle.remove(`${this.$attrSelector}-${this.$style?.name}`)}_themeChangeListener(e,t=()=>{}){this._offThemeChangeListener(e),jd.clearLoadedStyleNames();let o=t.bind(this);this.themeChangeListenerMap.set(e,o),Ce$1.on(`theme:change`,o)}_removeThemeListeners(){this._offThemeChangeListener(`_themeScopedListener`),this._offThemeChangeListener(`_loadCoreStyles`),this._offThemeChangeListener(`_load`)}_offThemeChangeListener(e){this.themeChangeListenerMap.has(e)&&(Ce$1.off(`theme:change`,this.themeChangeListenerMap.get(e)),this.themeChangeListenerMap.delete(e))}_getPTValue(e={},t=``,o={},r=!0){let s=/./g.test(t)&&!!o[t.split(`.`)[0]],{mergeSections:a=!0,mergeProps:d=!1}=this._getPropValue(`ptOptions`)?.()||this.config?.ptOptions?.()||{},c=r?s?this._useGlobalPT(this._getPTClassValue,t,o):this._useDefaultPT(this._getPTClassValue,t,o):void 0,l=s?void 0:this._usePT(this._getPT(e,this.$hostName||this.$name),this._getPTClassValue,t,W(G$1({},o),{global:c||{}})),b=this._getPTDatasets(t);return a||!a&&l?d?this._mergeProps(d,c,l,b):G$1(G$1(G$1({},c),l),b):G$1(G$1({},l),b)}_getPTDatasets(e=``){let t=`data-pc-`,o=e===`root`&&j(this.$pt()?.[`data-pc-section`]);return e!==`transition`&&W(G$1({},e===`root`&&W(G$1({[`${t}name`]:yi(o?this.$pt()?.[`data-pc-section`]:this.$name)},o&&{[`${t}extend`]:yi(this.$name)}),{[`${this.$attrSelector}`]:``})),{[`${t}section`]:yi(e.includes(`.`)?e.split(`.`).at(-1)??``:e)})}_getPTClassValue(e,t,o){let r=this._getOptionValue(e,t,o);return xe(r)||Wo$1(r)?{class:r}:r}_getPT(e,t=``,o){let r=(s,a=!1)=>{let d=o?o(s):s,c=yi(t),l=yi(this.$hostName||this.$name);return(a?c!==l?d?.[c]:void 0:d?.[c])??d};return e!=null&&Object.prototype.hasOwnProperty.call(e,`_usept`)?{_usept:e._usept,originalValue:r(e.originalValue),value:r(e.value)}:r(e,!0)}_usePT(e,t,o,r){let s=a=>t?.call(this,a,o,r);if(e!=null&&Object.prototype.hasOwnProperty.call(e,`_usept`)){let{mergeSections:a=!0,mergeProps:d=!1}=e._usept||this.config?.ptOptions()||{},c=s(e.originalValue),l=s(e.value);return c===void 0&&l===void 0?void 0:xe(l)?l:xe(c)?c:a||!a&&l?d?this._mergeProps(d,c,l):G$1(G$1({},c),l):l}return s(e)}_useGlobalPT(e,t,o){return this._usePT(this.$globalPT,e,t,o)}_useDefaultPT(e,t,o){return this._usePT(this.$defaultPT,e,t,o)}ptm(e=``,t={}){return this._getPTValue(this.$pt(),e,G$1(G$1({},this.$params),t))}ptms(e,t={}){return e.reduce((o,r)=>(o=cn(o,this.ptm(r,t))||{},o),{})}ptmo(e={},t=``,o={}){return this._getPTValue(e,t,G$1({instance:this},o),!1)}cx(e,t={}){return this.$unstyled()?void 0:Le(this._getOptionValue(this.$style.classes,e,G$1(G$1({},this.$params),t)))}sx(e=``,t=!0,o={}){if(t){let r=this._getOptionValue(this.$style.inlineStyles,e,G$1(G$1({},this.$params),o));return G$1(G$1({},this._getOptionValue(this.baseComponentStyle.inlineStyles,e,G$1(G$1({},this.$params),o))),r)}}translate(e,t){let o=this.config.getTranslation(e);return t?o?.[t]:o}static ɵfac=function(t){return new(t||n)};static ɵdir=CI({type:n,inputs:{dt:[1,`dt`],unstyled:[1,`unstyled`],pt:[1,`pt`],ptOptions:[1,`ptOptions`]},features:[QE([Ai,$i$1]),Kg]})}return n})();var $=(()=>{class n{pBind=mL(void 0);_attrs=_o$1(void 0);attrs=sD(()=>this._attrs()||this.pBind());styles=sD(()=>this.attrs()?.style);classes=sD(()=>Le(this.attrs()?.class));listeners=[];el=C(dr$1);renderer=C(ma$1);constructor(){hu(()=>{let e=this.attrs()||{},t=Object.fromEntries(Object.entries(e).filter(([o])=>o!==`style`&&o!==`class`));for(let[o,r]of Object.entries(t))if(o.startsWith(`on`)&&typeof r==`function`){let s=o.slice(2).toLowerCase();if(!this.listeners.some(a=>a.eventName===s)){let a=this.renderer.listen(this.el.nativeElement,s,r);this.listeners.push({eventName:s,unlisten:a})}}else r==null?this.renderer.removeAttribute(this.el.nativeElement,o):(this.renderer.setAttribute(this.el.nativeElement,o,r.toString()),o in this.el.nativeElement&&(this.el.nativeElement[o]=r))})}ngOnDestroy(){this.clearListeners()}setAttrs(e){jl(this._attrs(),e)||this._attrs.set(e)}clearListeners(){this.listeners.forEach(({unlisten:e})=>e()),this.listeners=[]}static ɵfac=function(t){return new(t||n)};static ɵdir=CI({type:n,selectors:[[``,`pBind`,``]],hostVars:4,hostBindings:function(t,o){t&2&&(NE(o.styles()),xE(o.classes()))},inputs:{pBind:[1,`pBind`]}})}return n})();var ve=(()=>{class n{static ɵfac=function(t){return new(t||n)};static ɵmod=DI({type:n});static ɵinj=Il({})}return n})();var nr=[`*`];var ir={root:`p-fluid`};var Oi=(()=>{class n extends $i$1{name=`fluid`;classes=ir;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var Fi=new _(`FLUID_INSTANCE`);var Bi=(()=>{class n extends G{componentName=`Fluid`;$pcFluid=C(Fi,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});_componentStyle=C(Oi);onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptms([`host`,`root`]))}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵcmp=II({type:n,selectors:[[`p-fluid`]],hostVars:2,hostBindings:function(t,o){t&2&&xE(o.cx(`root`))},features:[QE([Oi,{provide:Fi,useExisting:n},{provide:X,useExisting:n}]),NI([$]),Yf],ngContentSelectors:nr,decls:1,vars:0,template:function(t,o){t&1&&(lE(),uE(0))},dependencies:[jt],encapsulation:2})}return n})();var or=`
    
    .p-ink {
        display: block;
        position: absolute;
        background: dt('ripple.background');
        border-radius: 100%;
        transform: scale(0);
        pointer-events: none;
    }

    .p-ink-active {
        animation: ripple 0.4s linear;
    }

    @keyframes ripple {
        100% {
            opacity: 0;
            transform: scale(2.5);
        }
    }


    /* For PrimeNG */
    .p-ripple {
        overflow: hidden;
        position: relative;
    }

    .p-ripple-disabled .p-ink {
        display: none !important;
    }

    @keyframes ripple {
        100% {
            opacity: 0;
            transform: scale(2.5);
        }
    }
`;var rr={root:`p-ink`};var Ri=(()=>{class n extends $i$1{name=`ripple`;style=or;classes=rr;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var pt=(()=>{class n extends G{componentName=`Ripple`;_componentStyle=C(Ri);animationListener;mouseDownListener;timeout;constructor(){super(),hu(()=>{fc(this.platformId)&&(this.config.ripple()?(this.create(),this.mouseDownListener=this.renderer.listen(this.el.nativeElement,`mousedown`,this.onMouseDown.bind(this))):this.remove())})}onMouseDown(e){let t=this.getInk();if(!t||this.document.defaultView?.getComputedStyle(t,null).display===`none`)return;if(this.$unstyled()||zl(t,`p-ink-active`),t.setAttribute(`data-p-ink-active`,`false`),!id(t)&&!ad(t)){let a=Math.max(Zl(this.el.nativeElement),od(this.el.nativeElement));t.style.height=a+`px`,t.style.width=a+`px`}let o=sd(this.el.nativeElement),r=e.pageX-o.left+this.document.body.scrollTop-ad(t)/2,s=e.pageY-o.top+this.document.body.scrollLeft-id(t)/2;this.renderer.setStyle(t,`top`,s+`px`),this.renderer.setStyle(t,`left`,r+`px`),this.$unstyled()||Vl(t,`p-ink-active`),t.setAttribute(`data-p-ink-active`,`true`),this.timeout=setTimeout(()=>{let a=this.getInk();a&&(this.$unstyled()||zl(a,`p-ink-active`),a.setAttribute(`data-p-ink-active`,`false`))},401)}getInk(){let e=this.el.nativeElement.children;for(let t=0;t<e.length;t++)if(typeof e[t].className==`string`&&e[t].className.indexOf(`p-ink`)!==-1)return e[t];return null}resetInk(){let e=this.getInk();e&&(this.$unstyled()||zl(e,`p-ink-active`),e.setAttribute(`data-p-ink-active`,`false`))}onAnimationEnd(e){this.timeout&&clearTimeout(this.timeout),this.$unstyled()||zl(e.currentTarget,`p-ink-active`),e.currentTarget.setAttribute(`data-p-ink-active`,`false`)}create(){let e=this.renderer.createElement(`span`);this.renderer.addClass(e,`p-ink`),this.renderer.appendChild(this.el.nativeElement,e),this.renderer.setAttribute(e,`data-p-ink`,`true`),this.renderer.setAttribute(e,`data-p-ink-active`,`false`),this.renderer.setAttribute(e,`aria-hidden`,`true`),this.renderer.setAttribute(e,`role`,`presentation`),this.animationListener||(this.animationListener=this.renderer.listen(e,`animationend`,this.onAnimationEnd.bind(this)))}remove(){let e=this.getInk();e&&(this.mouseDownListener&&this.mouseDownListener(),this.animationListener&&this.animationListener(),this.mouseDownListener=null,this.animationListener=null,ud(e))}onDestroy(){this.config&&this.config.ripple()&&this.remove()}static ɵfac=function(t){return new(t||n)};static ɵdir=CI({type:n,selectors:[[``,`pRipple`,``]],hostAttrs:[1,`p-ripple`],features:[QE([Ri]),Yf]})}return n})();var ji=(()=>{class n{static ɵfac=function(t){return new(t||n)};static ɵmod=DI({type:n});static ɵinj=Il({})}return n})();var Hi=`
    .p-button {
        display: inline-flex;
        cursor: pointer;
        user-select: none;
        align-items: center;
        justify-content: center;
        overflow: hidden;
        position: relative;
        color: dt('button.primary.color');
        background: dt('button.primary.background');
        border: 1px solid dt('button.primary.border.color');
        padding: dt('button.padding.y') dt('button.padding.x');
        font-size: dt('button.font.size');
        font-weight: dt('button.label.font.weight');
        transition:
            background dt('button.transition.duration'),
            color dt('button.transition.duration'),
            border-color dt('button.transition.duration'),
            outline-color dt('button.transition.duration'),
            box-shadow dt('button.transition.duration');
        border-radius: dt('button.border.radius');
        outline-color: transparent;
        gap: dt('button.gap');
    }

    .p-button:disabled {
        cursor: default;
    }

    .p-button-icon-right {
        order: 1;
    }

    .p-button-icon-right:dir(rtl) {
        order: -1;
    }

    .p-button:not(.p-button-vertical) .p-button-icon:not(.p-button-icon-right):dir(rtl) {
        order: 1;
    }

    .p-button-icon-bottom {
        order: 2;
    }

    .p-button-icon-only {
        width: dt('button.icon.only.width');
        padding-inline-start: 0;
        padding-inline-end: 0;
        gap: 0;
    }

    .p-button-icon-only.p-button-rounded {
        border-radius: 50%;
        height: dt('button.icon.only.width');
    }

    .p-button-icon-only .p-button-label {
        visibility: hidden;
        width: 0;
    }

    .p-button-icon-only::after {
        content: "\xA0";
        visibility: hidden;
        width: 0;
    }

    .p-button-sm {
        font-size: dt('button.sm.font.size');
        padding: dt('button.sm.padding.y') dt('button.sm.padding.x');
    }

    .p-button-sm .p-button-icon {
        font-size: dt('button.sm.font.size');
    }

    .p-button-sm.p-button-icon-only {
        width: dt('button.sm.icon.only.width');
    }

    .p-button-sm.p-button-icon-only.p-button-rounded {
        height: dt('button.sm.icon.only.width');
    }

    .p-button-lg {
        font-size: dt('button.lg.font.size');
        padding: dt('button.lg.padding.y') dt('button.lg.padding.x');
    }

    .p-button-lg .p-button-icon {
        font-size: dt('button.lg.font.size');
    }

    .p-button-lg.p-button-icon-only {
        width: dt('button.lg.icon.only.width');
    }

    .p-button-lg.p-button-icon-only.p-button-rounded {
        height: dt('button.lg.icon.only.width');
    }

    .p-button-vertical {
        flex-direction: column;
    }

    .p-button-label {
        font-weight: dt('button.label.font.weight');
    }

    .p-button-fluid {
        width: 100%;
    }

    .p-button-fluid.p-button-icon-only {
        width: dt('button.icon.only.width');
    }

    .p-button:not(:disabled):hover {
        background: dt('button.primary.hover.background');
        border: 1px solid dt('button.primary.hover.border.color');
        color: dt('button.primary.hover.color');
    }

    .p-button:not(:disabled):active {
        background: dt('button.primary.active.background');
        border: 1px solid dt('button.primary.active.border.color');
        color: dt('button.primary.active.color');
    }

    .p-button:focus-visible {
        box-shadow: dt('button.primary.focus.ring.shadow');
        outline: dt('button.focus.ring.width') dt('button.focus.ring.style') dt('button.primary.focus.ring.color');
        outline-offset: dt('button.focus.ring.offset');
    }

    .p-button .p-badge {
        min-width: dt('button.badge.size');
        height: dt('button.badge.size');
        line-height: dt('button.badge.size');
    }

    .p-button-raised {
        box-shadow: dt('button.raised.shadow');
    }

    .p-button-rounded {
        border-radius: dt('button.rounded.border.radius');
    }

    .p-button-secondary {
        background: dt('button.secondary.background');
        border: 1px solid dt('button.secondary.border.color');
        color: dt('button.secondary.color');
    }

    .p-button-secondary:not(:disabled):hover {
        background: dt('button.secondary.hover.background');
        border: 1px solid dt('button.secondary.hover.border.color');
        color: dt('button.secondary.hover.color');
    }

    .p-button-secondary:not(:disabled):active {
        background: dt('button.secondary.active.background');
        border: 1px solid dt('button.secondary.active.border.color');
        color: dt('button.secondary.active.color');
    }

    .p-button-secondary:focus-visible {
        outline-color: dt('button.secondary.focus.ring.color');
        box-shadow: dt('button.secondary.focus.ring.shadow');
    }

    .p-button-success {
        background: dt('button.success.background');
        border: 1px solid dt('button.success.border.color');
        color: dt('button.success.color');
    }

    .p-button-success:not(:disabled):hover {
        background: dt('button.success.hover.background');
        border: 1px solid dt('button.success.hover.border.color');
        color: dt('button.success.hover.color');
    }

    .p-button-success:not(:disabled):active {
        background: dt('button.success.active.background');
        border: 1px solid dt('button.success.active.border.color');
        color: dt('button.success.active.color');
    }

    .p-button-success:focus-visible {
        outline-color: dt('button.success.focus.ring.color');
        box-shadow: dt('button.success.focus.ring.shadow');
    }

    .p-button-info {
        background: dt('button.info.background');
        border: 1px solid dt('button.info.border.color');
        color: dt('button.info.color');
    }

    .p-button-info:not(:disabled):hover {
        background: dt('button.info.hover.background');
        border: 1px solid dt('button.info.hover.border.color');
        color: dt('button.info.hover.color');
    }

    .p-button-info:not(:disabled):active {
        background: dt('button.info.active.background');
        border: 1px solid dt('button.info.active.border.color');
        color: dt('button.info.active.color');
    }

    .p-button-info:focus-visible {
        outline-color: dt('button.info.focus.ring.color');
        box-shadow: dt('button.info.focus.ring.shadow');
    }

    .p-button-warn {
        background: dt('button.warn.background');
        border: 1px solid dt('button.warn.border.color');
        color: dt('button.warn.color');
    }

    .p-button-warn:not(:disabled):hover {
        background: dt('button.warn.hover.background');
        border: 1px solid dt('button.warn.hover.border.color');
        color: dt('button.warn.hover.color');
    }

    .p-button-warn:not(:disabled):active {
        background: dt('button.warn.active.background');
        border: 1px solid dt('button.warn.active.border.color');
        color: dt('button.warn.active.color');
    }

    .p-button-warn:focus-visible {
        outline-color: dt('button.warn.focus.ring.color');
        box-shadow: dt('button.warn.focus.ring.shadow');
    }

    .p-button-help {
        background: dt('button.help.background');
        border: 1px solid dt('button.help.border.color');
        color: dt('button.help.color');
    }

    .p-button-help:not(:disabled):hover {
        background: dt('button.help.hover.background');
        border: 1px solid dt('button.help.hover.border.color');
        color: dt('button.help.hover.color');
    }

    .p-button-help:not(:disabled):active {
        background: dt('button.help.active.background');
        border: 1px solid dt('button.help.active.border.color');
        color: dt('button.help.active.color');
    }

    .p-button-help:focus-visible {
        outline-color: dt('button.help.focus.ring.color');
        box-shadow: dt('button.help.focus.ring.shadow');
    }

    .p-button-danger {
        background: dt('button.danger.background');
        border: 1px solid dt('button.danger.border.color');
        color: dt('button.danger.color');
    }

    .p-button-danger:not(:disabled):hover {
        background: dt('button.danger.hover.background');
        border: 1px solid dt('button.danger.hover.border.color');
        color: dt('button.danger.hover.color');
    }

    .p-button-danger:not(:disabled):active {
        background: dt('button.danger.active.background');
        border: 1px solid dt('button.danger.active.border.color');
        color: dt('button.danger.active.color');
    }

    .p-button-danger:focus-visible {
        outline-color: dt('button.danger.focus.ring.color');
        box-shadow: dt('button.danger.focus.ring.shadow');
    }

    .p-button-contrast {
        background: dt('button.contrast.background');
        border: 1px solid dt('button.contrast.border.color');
        color: dt('button.contrast.color');
    }

    .p-button-contrast:not(:disabled):hover {
        background: dt('button.contrast.hover.background');
        border: 1px solid dt('button.contrast.hover.border.color');
        color: dt('button.contrast.hover.color');
    }

    .p-button-contrast:not(:disabled):active {
        background: dt('button.contrast.active.background');
        border: 1px solid dt('button.contrast.active.border.color');
        color: dt('button.contrast.active.color');
    }

    .p-button-contrast:focus-visible {
        outline-color: dt('button.contrast.focus.ring.color');
        box-shadow: dt('button.contrast.focus.ring.shadow');
    }

    .p-button-outlined {
        background: transparent;
        border-color: dt('button.outlined.primary.border.color');
        color: dt('button.outlined.primary.color');
    }

    .p-button-outlined:not(:disabled):hover {
        background: dt('button.outlined.primary.hover.background');
        border-color: dt('button.outlined.primary.border.color');
        color: dt('button.outlined.primary.color');
    }

    .p-button-outlined:not(:disabled):active {
        background: dt('button.outlined.primary.active.background');
        border-color: dt('button.outlined.primary.border.color');
        color: dt('button.outlined.primary.color');
    }

    .p-button-outlined.p-button-secondary {
        border-color: dt('button.outlined.secondary.border.color');
        color: dt('button.outlined.secondary.color');
    }

    .p-button-outlined.p-button-secondary:not(:disabled):hover {
        background: dt('button.outlined.secondary.hover.background');
        border-color: dt('button.outlined.secondary.border.color');
        color: dt('button.outlined.secondary.color');
    }

    .p-button-outlined.p-button-secondary:not(:disabled):active {
        background: dt('button.outlined.secondary.active.background');
        border-color: dt('button.outlined.secondary.border.color');
        color: dt('button.outlined.secondary.color');
    }

    .p-button-outlined.p-button-success {
        border-color: dt('button.outlined.success.border.color');
        color: dt('button.outlined.success.color');
    }

    .p-button-outlined.p-button-success:not(:disabled):hover {
        background: dt('button.outlined.success.hover.background');
        border-color: dt('button.outlined.success.border.color');
        color: dt('button.outlined.success.color');
    }

    .p-button-outlined.p-button-success:not(:disabled):active {
        background: dt('button.outlined.success.active.background');
        border-color: dt('button.outlined.success.border.color');
        color: dt('button.outlined.success.color');
    }

    .p-button-outlined.p-button-info {
        border-color: dt('button.outlined.info.border.color');
        color: dt('button.outlined.info.color');
    }

    .p-button-outlined.p-button-info:not(:disabled):hover {
        background: dt('button.outlined.info.hover.background');
        border-color: dt('button.outlined.info.border.color');
        color: dt('button.outlined.info.color');
    }

    .p-button-outlined.p-button-info:not(:disabled):active {
        background: dt('button.outlined.info.active.background');
        border-color: dt('button.outlined.info.border.color');
        color: dt('button.outlined.info.color');
    }

    .p-button-outlined.p-button-warn {
        border-color: dt('button.outlined.warn.border.color');
        color: dt('button.outlined.warn.color');
    }

    .p-button-outlined.p-button-warn:not(:disabled):hover {
        background: dt('button.outlined.warn.hover.background');
        border-color: dt('button.outlined.warn.border.color');
        color: dt('button.outlined.warn.color');
    }

    .p-button-outlined.p-button-warn:not(:disabled):active {
        background: dt('button.outlined.warn.active.background');
        border-color: dt('button.outlined.warn.border.color');
        color: dt('button.outlined.warn.color');
    }

    .p-button-outlined.p-button-help {
        border-color: dt('button.outlined.help.border.color');
        color: dt('button.outlined.help.color');
    }

    .p-button-outlined.p-button-help:not(:disabled):hover {
        background: dt('button.outlined.help.hover.background');
        border-color: dt('button.outlined.help.border.color');
        color: dt('button.outlined.help.color');
    }

    .p-button-outlined.p-button-help:not(:disabled):active {
        background: dt('button.outlined.help.active.background');
        border-color: dt('button.outlined.help.border.color');
        color: dt('button.outlined.help.color');
    }

    .p-button-outlined.p-button-danger {
        border-color: dt('button.outlined.danger.border.color');
        color: dt('button.outlined.danger.color');
    }

    .p-button-outlined.p-button-danger:not(:disabled):hover {
        background: dt('button.outlined.danger.hover.background');
        border-color: dt('button.outlined.danger.border.color');
        color: dt('button.outlined.danger.color');
    }

    .p-button-outlined.p-button-danger:not(:disabled):active {
        background: dt('button.outlined.danger.active.background');
        border-color: dt('button.outlined.danger.border.color');
        color: dt('button.outlined.danger.color');
    }

    .p-button-outlined.p-button-contrast {
        border-color: dt('button.outlined.contrast.border.color');
        color: dt('button.outlined.contrast.color');
    }

    .p-button-outlined.p-button-contrast:not(:disabled):hover {
        background: dt('button.outlined.contrast.hover.background');
        border-color: dt('button.outlined.contrast.border.color');
        color: dt('button.outlined.contrast.color');
    }

    .p-button-outlined.p-button-contrast:not(:disabled):active {
        background: dt('button.outlined.contrast.active.background');
        border-color: dt('button.outlined.contrast.border.color');
        color: dt('button.outlined.contrast.color');
    }

    .p-button-outlined.p-button-plain {
        border-color: dt('button.outlined.plain.border.color');
        color: dt('button.outlined.plain.color');
    }

    .p-button-outlined.p-button-plain:not(:disabled):hover {
        background: dt('button.outlined.plain.hover.background');
        border-color: dt('button.outlined.plain.border.color');
        color: dt('button.outlined.plain.color');
    }

    .p-button-outlined.p-button-plain:not(:disabled):active {
        background: dt('button.outlined.plain.active.background');
        border-color: dt('button.outlined.plain.border.color');
        color: dt('button.outlined.plain.color');
    }

    .p-button-text {
        background: transparent;
        border-color: transparent;
        color: dt('button.text.primary.color');
    }

    .p-button-text:not(:disabled):hover {
        background: dt('button.text.primary.hover.background');
        border-color: transparent;
        color: dt('button.text.primary.color');
    }

    .p-button-text:not(:disabled):active {
        background: dt('button.text.primary.active.background');
        border-color: transparent;
        color: dt('button.text.primary.color');
    }

    .p-button-text.p-button-secondary {
        background: transparent;
        border-color: transparent;
        color: dt('button.text.secondary.color');
    }

    .p-button-text.p-button-secondary:not(:disabled):hover {
        background: dt('button.text.secondary.hover.background');
        border-color: transparent;
        color: dt('button.text.secondary.color');
    }

    .p-button-text.p-button-secondary:not(:disabled):active {
        background: dt('button.text.secondary.active.background');
        border-color: transparent;
        color: dt('button.text.secondary.color');
    }

    .p-button-text.p-button-success {
        background: transparent;
        border-color: transparent;
        color: dt('button.text.success.color');
    }

    .p-button-text.p-button-success:not(:disabled):hover {
        background: dt('button.text.success.hover.background');
        border-color: transparent;
        color: dt('button.text.success.color');
    }

    .p-button-text.p-button-success:not(:disabled):active {
        background: dt('button.text.success.active.background');
        border-color: transparent;
        color: dt('button.text.success.color');
    }

    .p-button-text.p-button-info {
        background: transparent;
        border-color: transparent;
        color: dt('button.text.info.color');
    }

    .p-button-text.p-button-info:not(:disabled):hover {
        background: dt('button.text.info.hover.background');
        border-color: transparent;
        color: dt('button.text.info.color');
    }

    .p-button-text.p-button-info:not(:disabled):active {
        background: dt('button.text.info.active.background');
        border-color: transparent;
        color: dt('button.text.info.color');
    }

    .p-button-text.p-button-warn {
        background: transparent;
        border-color: transparent;
        color: dt('button.text.warn.color');
    }

    .p-button-text.p-button-warn:not(:disabled):hover {
        background: dt('button.text.warn.hover.background');
        border-color: transparent;
        color: dt('button.text.warn.color');
    }

    .p-button-text.p-button-warn:not(:disabled):active {
        background: dt('button.text.warn.active.background');
        border-color: transparent;
        color: dt('button.text.warn.color');
    }

    .p-button-text.p-button-help {
        background: transparent;
        border-color: transparent;
        color: dt('button.text.help.color');
    }

    .p-button-text.p-button-help:not(:disabled):hover {
        background: dt('button.text.help.hover.background');
        border-color: transparent;
        color: dt('button.text.help.color');
    }

    .p-button-text.p-button-help:not(:disabled):active {
        background: dt('button.text.help.active.background');
        border-color: transparent;
        color: dt('button.text.help.color');
    }

    .p-button-text.p-button-danger {
        background: transparent;
        border-color: transparent;
        color: dt('button.text.danger.color');
    }

    .p-button-text.p-button-danger:not(:disabled):hover {
        background: dt('button.text.danger.hover.background');
        border-color: transparent;
        color: dt('button.text.danger.color');
    }

    .p-button-text.p-button-danger:not(:disabled):active {
        background: dt('button.text.danger.active.background');
        border-color: transparent;
        color: dt('button.text.danger.color');
    }

    .p-button-text.p-button-contrast {
        background: transparent;
        border-color: transparent;
        color: dt('button.text.contrast.color');
    }

    .p-button-text.p-button-contrast:not(:disabled):hover {
        background: dt('button.text.contrast.hover.background');
        border-color: transparent;
        color: dt('button.text.contrast.color');
    }

    .p-button-text.p-button-contrast:not(:disabled):active {
        background: dt('button.text.contrast.active.background');
        border-color: transparent;
        color: dt('button.text.contrast.color');
    }

    .p-button-text.p-button-plain {
        background: transparent;
        border-color: transparent;
        color: dt('button.text.plain.color');
    }

    .p-button-text.p-button-plain:not(:disabled):hover {
        background: dt('button.text.plain.hover.background');
        border-color: transparent;
        color: dt('button.text.plain.color');
    }

    .p-button-text.p-button-plain:not(:disabled):active {
        background: dt('button.text.plain.active.background');
        border-color: transparent;
        color: dt('button.text.plain.color');
    }

    .p-button-link {
        background: transparent;
        border-color: transparent;
        color: dt('button.link.color');
    }

    .p-button-link:not(:disabled):hover {
        background: transparent;
        border-color: transparent;
        color: dt('button.link.hover.color');
    }

    .p-button-link:not(:disabled):hover .p-button-label {
        text-decoration: underline;
    }

    .p-button-link:not(:disabled):active {
        background: transparent;
        border-color: transparent;
        color: dt('button.link.active.color');
    }
`;function Vi(n){if(!(n==null||n===``))return typeof n==`number`||/^\d+(\.\d+)?$/.test(n)?`${n}px`:`${n}`}var Xe=(()=>{class n{_iconSignal=_o$1(null);get _icon(){return this._iconSignal()}set _icon(e){this._iconSignal.set(e)}size=mL(void 0);color=mL(void 0);styleClass=mL(void 0);spin=mL(void 0);iconNodes=sD(()=>this._iconSignal()?.nodes??[]);computedSize=sD(()=>this.size()??20);computedClass=sD(()=>{let e=this._iconSignal();return Le(`p-icon`,e?.name&&`p-icon-${e.name}`,this.spin()&&`p-icon-spin`,this.styleClass())});get hostWidth(){return this.computedSize()}get hostHeight(){return this.computedSize()}get hostViewBox(){return this._iconSignal()?.svg?.viewBox}get hostFill(){return this._iconSignal()?.svg?.fill}get hostXmlns(){return this._iconSignal()?.svg?.xmlns}hostAriaHidden=`true`;get hostClass(){return this.computedClass()}get hostColor(){return this.color()||null}get hostIconSize(){return Vi(this.size())??null}static ɵfac=function(t){return new(t||n)};static ɵdir=CI({type:n,hostVars:12,hostBindings:function(t,o){t&2&&(np(`width`,o.hostWidth)(`height`,o.hostHeight)(`viewBox`,o.hostViewBox)(`fill`,o.hostFill)(`xmlns`,o.hostXmlns)(`aria-hidden`,o.hostAriaHidden),xE(o.hostClass),Ep(`color`,o.hostColor)(`--%NS%px-icon-size`,o.hostIconSize))},inputs:{size:[1,`size`],color:[1,`color`],styleClass:[1,`styleClass`],spin:[1,`spin`]}})}return n})();var zi={name:`spinner`,meta:{tags:[`spinner`,`loading`,`process`,`wait`,`buffering`]},svg:{xmlns:`http://www.w3.org/2000/svg`,width:20,height:20,viewBox:`0 0 20 20`,fill:`none`},nodes:[[`path`,{d:`M1 10C1 5.02579 5.02579 1 10 1C12.3905 1 14.562 1.9393 16.1738 3.45312C16.4756 3.73669 16.4905 4.21178 16.207 4.51367C15.9235 4.81558 15.4484 4.83039 15.1465 4.54688C13.7983 3.2807 11.9895 2.5 10 2.5C5.85421 2.5 2.5 5.85421 2.5 10C2.5 14.1458 5.85421 17.5 10 17.5C14.1458 17.5 17.5 14.1458 17.5 10C17.5 9.58579 17.8358 9.25 18.25 9.25C18.6642 9.25 19 9.58579 19 10C19 14.9742 14.9742 19 10 19C5.02579 19 1 14.9742 1 10Z`,fill:`currentColor`,key:`p4wko0`}]]};var ar=(n,i)=>i[1].key||n;function lr(n,i){if(n&1&&(ou(),ip(0,`path`)),n&2){let e=aE().$implicit;np(`d`,e[1].d)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`fill-rule`,e[1].fillRule)(`clip-rule`,e[1].clipRule)(`stroke`,e[1].stroke)(`stroke-width`,e[1].strokeWidth)(`stroke-opacity`,e[1].strokeOpacity)(`opacity`,e[1].opacity)}}function dr(n,i){if(n&1&&(ou(),ip(0,`circle`)),n&2){let e=aE().$implicit;np(`cx`,e[1].cx)(`cy`,e[1].cy)(`r`,e[1].r)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function cr(n,i){if(n&1&&(ou(),ip(0,`rect`)),n&2){let e=aE().$implicit;np(`x`,e[1].x)(`y`,e[1].y)(`width`,e[1].width)(`height`,e[1].height)(`rx`,e[1].rx)(`ry`,e[1].ry)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function ur(n,i){if(n&1&&(ou(),ip(0,`line`)),n&2){let e=aE().$implicit;np(`x1`,e[1].x1)(`y1`,e[1].y1)(`x2`,e[1].x2)(`y2`,e[1].y2)(`stroke`,e[1].stroke)(`stroke-opacity`,e[1].strokeOpacity)(`opacity`,e[1].opacity)}}function pr(n,i){if(n&1&&(ou(),ip(0,`polyline`)),n&2){let e=aE().$implicit;np(`points`,e[1].points)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function fr(n,i){if(n&1&&(ou(),ip(0,`polygon`)),n&2){let e=aE().$implicit;np(`points`,e[1].points)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function hr(n,i){if(n&1&&(ou(),ip(0,`ellipse`)),n&2){let e=aE().$implicit;np(`cx`,e[1].cx)(`cy`,e[1].cy)(`rx`,e[1].rx)(`ry`,e[1].ry)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function mr(n,i){if(n&1&&qI(0,lr,1,9,`:svg:path`)(1,dr,1,6,`:svg:circle`)(2,cr,1,9,`:svg:rect`)(3,ur,1,7,`:svg:line`)(4,pr,1,4,`:svg:polyline`)(5,fr,1,4,`:svg:polygon`)(6,hr,1,7,`:svg:ellipse`),n&2){let e,t=i.$implicit;GI((e=t[0])===`path`?0:e===`circle`?1:e===`rect`?2:e===`line`?3:e===`polyline`?4:e===`polygon`?5:e===`ellipse`?6:-1)}}var Wi=(()=>{class n extends Xe{constructor(){super(),this._icon=zi}static ɵfac=function(t){return new(t||n)};static ɵcmp=II({type:n,selectors:[[`svg`,`data-p-icon`,`spinner`]],features:[Yf],decls:2,vars:0,template:function(t,o){t&1&&zI(0,mr,7,1,null,null,ar),t&2&&QI(o.iconNodes())},encapsulation:2,changeDetection:1})}return n})();var un=(()=>{class n{static zindex=1e3;static calculatedScrollbarWidth=null;static calculatedScrollbarHeight=null;static browser;static addClass(e,t){e&&t&&(e.classList?e.classList.add(t):e.className+=` `+t)}static addMultipleClasses(e,t){if(e&&t)if(e.classList){let o=t.trim().split(` `);for(let r=0;r<o.length;r++)e.classList.add(o[r])}else{let o=t.split(` `);for(let r=0;r<o.length;r++)e.className+=` `+o[r]}}static removeClass(e,t){e&&t&&(e.classList?e.classList.remove(t):e.className=e.className.replace(new RegExp(`(^|\\b)`+t.split(` `).join(`|`)+`(\\b|$)`,`gi`),` `))}static removeMultipleClasses(e,t){e&&t&&[t].flat().filter(Boolean).forEach(o=>o.split(` `).forEach(r=>this.removeClass(e,r)))}static hasClass(e,t){return e&&t?e.classList?e.classList.contains(t):new RegExp(`(^| )`+t+`( |$)`,`gi`).test(e.className):!1}static siblings(e){return Array.prototype.filter.call(e.parentNode.children,function(t){return t!==e})}static find(e,t){return Array.from(e.querySelectorAll(t))}static findSingle(e,t){return this.isElement(e)?e.querySelector(t):null}static index(e){let t=e.parentNode.childNodes,o=0;for(let r=0;r<t.length;r++){if(t[r]==e)return o;t[r].nodeType==1&&o++}return-1}static indexWithinGroup(e,t){let o=e.parentNode?e.parentNode.childNodes:[],r=0;for(let s=0;s<o.length;s++){if(o[s]==e)return r;o[s].attributes&&o[s].attributes[t]&&o[s].nodeType==1&&r++}return-1}static appendOverlay(e,t,o=`self`){o!==`self`&&e&&t&&this.appendChild(e,t)}static alignOverlay(e,t,o=`self`,r=!0){e&&t&&(r&&(e.style.minWidth=`${n.getOuterWidth(t)}px`),o===`self`?this.relativePosition(e,t):this.absolutePosition(e,t))}static relativePosition(e,t,o=!0){let r=S=>{if(S)return getComputedStyle(S).getPropertyValue(`position`)===`relative`?S:r(S.parentElement)},s=e.offsetParent?{width:e.offsetWidth,height:e.offsetHeight}:this.getHiddenElementDimensions(e),a=t.offsetHeight,d=t.getBoundingClientRect(),c=this.getWindowScrollTop(),l=this.getWindowScrollLeft(),b=this.getViewport(),h=r(e)?.getBoundingClientRect()||{top:-1*c,left:-1*l},M,j,_=`top`;d.top+a+s.height>b.height?(M=d.top-h.top-s.height,_=`bottom`,d.top+M<0&&(M=-1*d.top)):(M=a+d.top-h.top,_=`top`);let I=d.left+s.width-b.width,F=d.left-h.left;if(s.width>b.width?j=(d.left-h.left)*-1:I>0?j=F-I:j=d.left-h.left,e.style.top=M+`px`,e.style.left=j+`px`,e.style.transformOrigin=_,o){let S=Wl(/-anchor-gutter$/)?.value;e.style.marginTop=_===`bottom`?`calc(${S??`2px`} * -1)`:S??``}}static absolutePosition(e,t,o=!0){let r=e.offsetParent?{width:e.offsetWidth,height:e.offsetHeight}:this.getHiddenElementDimensions(e),s=r.height,a=r.width,d=t.offsetHeight,c=t.offsetWidth,l=t.getBoundingClientRect(),b=this.getWindowScrollTop(),x=this.getWindowScrollLeft(),h=this.getViewport(),M,j;l.top+d+s>h.height?(M=l.top+b-s,e.style.transformOrigin=`bottom`,M<0&&(M=b)):(M=d+l.top+b,e.style.transformOrigin=`top`),l.left+a>h.width?j=Math.max(0,l.left+x+c-a):j=l.left+x,e.style.top=M+`px`,e.style.left=j+`px`,o&&(e.style.marginTop=origin===`bottom`?`calc(var(--p-anchor-gutter) * -1)`:`calc(var(--p-anchor-gutter))`)}static getParents(e,t=[]){let o=e.parentNode instanceof ShadowRoot?e.parentNode.host:e.parentNode;return o==null?t:this.getParents(o,t.concat([o]))}static getScrollableParents(e){let t=[];if(e){let o=this.getParents(e),r=/(auto|scroll)/,s=a=>{let d=window.getComputedStyle(a,null);return r.test(d.getPropertyValue(`overflow`))||r.test(d.getPropertyValue(`overflowX`))||r.test(d.getPropertyValue(`overflowY`))};for(let a of o){let d=a.nodeType===1&&a.dataset.scrollselectors;if(d){let c=d.split(`,`);for(let l of c){let b=this.findSingle(a,l);b&&s(b)&&t.push(b)}}a.nodeType!==9&&s(a)&&t.push(a)}}return t}static getHiddenElementOuterHeight(e){e.style.visibility=`hidden`,e.style.display=`block`;let t=e.offsetHeight;return e.style.display=`none`,e.style.visibility=`visible`,t}static getHiddenElementOuterWidth(e){e.style.visibility=`hidden`,e.style.display=`block`;let t=e.offsetWidth;return e.style.display=`none`,e.style.visibility=`visible`,t}static getHiddenElementDimensions(e){let t={};return e.style.visibility=`hidden`,e.style.display=`block`,t.width=e.offsetWidth,t.height=e.offsetHeight,e.style.display=`none`,e.style.visibility=`visible`,t}static scrollInView(e,t){let o=getComputedStyle(e).getPropertyValue(`borderTopWidth`),r=o?parseFloat(o):0,s=getComputedStyle(e).getPropertyValue(`paddingTop`),a=s?parseFloat(s):0,d=e.getBoundingClientRect(),l=t.getBoundingClientRect().top+document.body.scrollTop-(d.top+document.body.scrollTop)-r-a,b=e.scrollTop,x=e.clientHeight,h=this.getOuterHeight(t);l<0?e.scrollTop=b+l:l+h>x&&(e.scrollTop=b+l-x+h)}static fadeIn(e,t){e.style.opacity=0;let o=+new Date,r=0,s=function(){r=+e.style.opacity.replace(`,`,`.`)+(new Date().getTime()-o)/t,e.style.opacity=r,o=+new Date,+r<1&&(window.requestAnimationFrame?window.requestAnimationFrame(s):setTimeout(s,16))};s()}static fadeOut(e,t){let o=1,r=50,a=r/t,d=setInterval(()=>{o=o-a,o<=0&&(o=0,clearInterval(d)),e.style.opacity=o},r)}static getWindowScrollTop(){let e=document.documentElement;return(window.pageYOffset||e.scrollTop)-(e.clientTop||0)}static getWindowScrollLeft(){let e=document.documentElement;return(window.pageXOffset||e.scrollLeft)-(e.clientLeft||0)}static matches(e,t){let o=Element.prototype;return(o.matches||o.webkitMatchesSelector||o.mozMatchesSelector||o.msMatchesSelector||function(s){return[].indexOf.call(document.querySelectorAll(s),this)!==-1}).call(e,t)}static getOuterWidth(e,t){let o=e.offsetWidth;if(t){let r=getComputedStyle(e);o+=parseFloat(r.marginLeft)+parseFloat(r.marginRight)}return o}static getHorizontalPadding(e){let t=getComputedStyle(e);return parseFloat(t.paddingLeft)+parseFloat(t.paddingRight)}static getHorizontalMargin(e){let t=getComputedStyle(e);return parseFloat(t.marginLeft)+parseFloat(t.marginRight)}static innerWidth(e){let t=e.offsetWidth,o=getComputedStyle(e);return t+=parseFloat(o.paddingLeft)+parseFloat(o.paddingRight),t}static width(e){let t=e.offsetWidth,o=getComputedStyle(e);return t-=parseFloat(o.paddingLeft)+parseFloat(o.paddingRight),t}static getInnerHeight(e){let t=e.offsetHeight,o=getComputedStyle(e);return t+=parseFloat(o.paddingTop)+parseFloat(o.paddingBottom),t}static getOuterHeight(e,t){let o=e.offsetHeight;if(t){let r=getComputedStyle(e);o+=parseFloat(r.marginTop)+parseFloat(r.marginBottom)}return o}static getHeight(e){let t=e.offsetHeight,o=getComputedStyle(e);return t-=parseFloat(o.paddingTop)+parseFloat(o.paddingBottom)+parseFloat(o.borderTopWidth)+parseFloat(o.borderBottomWidth),t}static getWidth(e){let t=e.offsetWidth,o=getComputedStyle(e);return t-=parseFloat(o.paddingLeft)+parseFloat(o.paddingRight)+parseFloat(o.borderLeftWidth)+parseFloat(o.borderRightWidth),t}static getViewport(){let e=window,t=document,o=t.documentElement,r=t.getElementsByTagName(`body`)[0];return{width:e.innerWidth||o.clientWidth||r.clientWidth,height:e.innerHeight||o.clientHeight||r.clientHeight}}static getOffset(e){let t=e.getBoundingClientRect();return{top:t.top+(window.pageYOffset||document.documentElement.scrollTop||document.body.scrollTop||0),left:t.left+(window.pageXOffset||document.documentElement.scrollLeft||document.body.scrollLeft||0)}}static replaceElementWith(e,t){let o=e.parentNode;if(!o)throw`Can't replace element`;return o.replaceChild(t,e)}static getUserAgent(){if(navigator&&this.isClient())return navigator.userAgent}static isIE(){let e=window.navigator.userAgent;return e.indexOf(`MSIE `)>0||e.indexOf(`Trident/`)>0||e.indexOf(`Edge/`)>0}static isIOS(){return/iPad|iPhone|iPod/.test(navigator.userAgent)&&!window.MSStream}static isAndroid(){return/(android)/i.test(navigator.userAgent)}static isTouchDevice(){return`ontouchstart`in window||navigator.maxTouchPoints>0}static appendChild(e,t){if(this.isElement(t))t.appendChild(e);else if(t&&t.el&&t.el.nativeElement)t.el.nativeElement.appendChild(e);else throw`Cannot append `+t+` to `+e}static removeChild(e,t){if(this.isElement(t))t.removeChild(e);else if(t.el&&t.el.nativeElement)t.el.nativeElement.removeChild(e);else throw`Cannot remove `+e+` from `+t}static removeElement(e){`remove`in Element.prototype?e.remove():e.parentNode?.removeChild(e)}static isElement(e){return typeof HTMLElement==`object`?e instanceof HTMLElement:e&&typeof e==`object`&&e!==null&&e.nodeType===1&&typeof e.nodeName==`string`}static calculateScrollbarWidth(e){if(e){let t=getComputedStyle(e);return e.offsetWidth-e.clientWidth-parseFloat(t.borderLeftWidth)-parseFloat(t.borderRightWidth)}else{if(this.calculatedScrollbarWidth!==null)return this.calculatedScrollbarWidth;let t=document.createElement(`div`);t.className=`p-scrollbar-measure`,document.body.appendChild(t);let o=t.offsetWidth-t.clientWidth;return document.body.removeChild(t),this.calculatedScrollbarWidth=o,o}}static calculateScrollbarHeight(){if(this.calculatedScrollbarHeight!==null)return this.calculatedScrollbarHeight;let e=document.createElement(`div`);e.className=`p-scrollbar-measure`,document.body.appendChild(e);let t=e.offsetHeight-e.clientHeight;return document.body.removeChild(e),this.calculatedScrollbarWidth=t,t}static invokeElementMethod(e,t,o){e[t].apply(e,o)}static clearSelection(){if(window.getSelection&&window.getSelection())window.getSelection()?.empty?window.getSelection()?.empty():window.getSelection()?.removeAllRanges&&(window.getSelection()?.rangeCount||0)>0&&(window.getSelection()?.getRangeAt(0)?.getClientRects()?.length||0)>0&&window.getSelection()?.removeAllRanges();else if(document.selection&&document.selection.empty)try{document.selection.empty()}catch{}}static getBrowser(){if(!this.browser){let e=this.resolveUserAgent();this.browser={},e.browser&&(this.browser[e.browser]=!0,this.browser.version=e.version),this.browser.chrome?this.browser.webkit=!0:this.browser.webkit&&(this.browser.safari=!0)}return this.browser}static resolveUserAgent(){let e=navigator.userAgent.toLowerCase(),t=/(chrome)[ /]([\w.]+)/.exec(e)||/(webkit)[ /]([\w.]+)/.exec(e)||/(opera)(?:.*version|)[ /]([\w.]+)/.exec(e)||/(msie) ([\w.]+)/.exec(e)||e.indexOf(`compatible`)<0&&/(mozilla)(?:.*? rv:([\w.]+)|)/.exec(e)||[];return{browser:t[1]||``,version:t[2]||`0`}}static isInteger(e){return Number.isInteger?Number.isInteger(e):typeof e==`number`&&isFinite(e)&&Math.floor(e)===e}static isHidden(e){return!e||e.offsetParent===null}static isVisible(e){return e&&e.offsetParent!=null}static isExist(e){return e!==null&&typeof e<`u`&&e.nodeName&&e.parentNode}static focus(e,t){e&&document.activeElement!==e&&e.focus(t)}static getFocusableSelectorString(e=``){return`button:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${e},
        [href][clientHeight][clientWidth]:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${e},
        input:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${e},
        select:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${e},
        textarea:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${e},
        [tabIndex]:not([tabIndex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${e},
        [contenteditable]:not([tabIndex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${e},
        .p-inputtext:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${e},
        .p-button:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${e}`}static getFocusableElements(e,t=``){let o=this.find(e,this.getFocusableSelectorString(t)),r=[];for(let s of o){let a=getComputedStyle(s);this.isVisible(s)&&a.display!=`none`&&a.visibility!=`hidden`&&r.push(s)}return r}static getFocusableElement(e,t=``){let o=this.findSingle(e,this.getFocusableSelectorString(t));if(o){let r=getComputedStyle(o);if(this.isVisible(o)&&r.display!=`none`&&r.visibility!=`hidden`)return o}return null}static getFirstFocusableElement(e,t=``){let o=this.getFocusableElements(e,t);return o.length>0?o[0]:null}static getLastFocusableElement(e,t){let o=this.getFocusableElements(e,t);return o.length>0?o[o.length-1]:null}static getNextFocusableElement(e,t=!1){let o=n.getFocusableElements(e),r=0;if(o&&o.length>0){let s=o.indexOf(o[0].ownerDocument.activeElement);t?s==-1||s===0?r=o.length-1:r=s-1:s!=-1&&s!==o.length-1&&(r=s+1)}return o[r]}static generateZIndex(){return this.zindex=this.zindex||999,++this.zindex}static getSelection(){return window.getSelection?window.getSelection()?.toString():document.getSelection?document.getSelection()?.toString():document.selection?document.selection.createRange().text:null}static getTargetElement(e,t){if(!e)return null;switch(e){case`document`:return document;case`window`:return window;case`@next`:return t?.nextElementSibling;case`@prev`:return t?.previousElementSibling;case`@parent`:return t?.parentElement;case`@grandparent`:return t?.parentElement?.parentElement;default:{let o=typeof e;if(o===`string`)return document.querySelector(e);if(o===`object`&&Object.prototype.hasOwnProperty.call(e,`nativeElement`))return this.isExist(e.nativeElement)?e.nativeElement:void 0;let s=(a=>!!(a&&a.constructor&&a.call&&a.apply))(e)?e():e;return s&&s.nodeType===9||this.isExist(s)?s:null}}}static isClient(){return!!(typeof window<`u`&&window.document&&window.document.createElement)}static getAttribute(e,t){if(e){let o=e.getAttribute(t);return isNaN(o)?o===`true`||o===`false`?o===`true`:o:+o}}static calculateBodyScrollbarWidth(){return window.innerWidth-document.documentElement.offsetWidth}static blockBodyScroll(e=`p-overflow-hidden`){document.body.style.setProperty(`--px-scrollbar-width`,this.calculateBodyScrollbarWidth()+`px`),this.addClass(document.body,e)}static unblockBodyScroll(e=`p-overflow-hidden`){document.body.style.removeProperty(`--px-scrollbar-width`),this.removeClass(document.body,e)}static createElement(e,t={},...o){if(e){let r=document.createElement(e);return this.setAttributes(r,t),r.append(...o),r}}static setAttribute(e,t=``,o){this.isElement(e)&&o!==null&&o!==void 0&&e.setAttribute(t,o)}static setAttributes(e,t={}){if(this.isElement(e)){let o=(r,s)=>{let a=e?.$attrs?.[r]?[e?.$attrs?.[r]]:[];return[s].flat().reduce((d,c)=>{if(c!=null){let l=typeof c;if(l===`string`||l===`number`)d.push(c);else if(l===`object`){let b=Array.isArray(c)?o(r,c):Object.entries(c).map(([x,h])=>r===`style`&&(h||h===0)?`${x.replace(/([a-z])([A-Z])/g,`$1-$2`).toLowerCase()}:${h}`:h?x:void 0);d=b.length?d.concat(b.filter(x=>!!x)):d}}return d},a)};Object.entries(t).forEach(([r,s])=>{if(s!=null){let a=r.match(/^on(.+)/);a?e.addEventListener(a[1].toLowerCase(),s):r===`pBind`?this.setAttributes(e,s):(s=r===`class`?[...new Set(o(`class`,s))].join(` `).trim():r===`style`?o(`style`,s).join(`;`).trim():s,(e.$attrs=e.$attrs||{})&&(e.$attrs[r]=s),e.setAttribute(r,s))}})}}static isFocusableElement(e,t=``){return this.isElement(e)?e.matches(`button:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${t},
                [href][clientHeight][clientWidth]:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${t},
                input:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${t},
                select:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${t},
                textarea:not([tabindex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${t},
                [tabIndex]:not([tabIndex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${t},
                [contenteditable]:not([tabIndex = "-1"]):not([disabled]):not([style*="display:none"]):not([hidden])${t}`):!1}}return n})();var Vt=class{element;listener;scrollableParents;constructor(i,e=()=>{}){this.element=i,this.listener=e}bindScrollListener(){this.scrollableParents=un.getScrollableParents(this.element);for(let i=0;i<this.scrollableParents.length;i++)this.scrollableParents[i].addEventListener(`scroll`,this.listener)}unbindScrollListener(){if(this.scrollableParents)for(let i=0;i<this.scrollableParents.length;i++)this.scrollableParents[i].removeEventListener(`scroll`,this.listener)}destroy(){this.unbindScrollListener(),this.element=null,this.listener=null,this.scrollableParents=null}};var Ui=(()=>{class n extends G{autofocus=mL(!1,{alias:`pAutoFocus`,transform:wL});focused=!1;host=C(dr$1);onAfterContentChecked(){this.autofocus()===!1?this.host.nativeElement.removeAttribute(`autofocus`):this.host.nativeElement.setAttribute(`autofocus`,!0),this.focused||this.autoFocus()}onAfterViewChecked(){this.focused||this.autoFocus()}autoFocus(){fc(this.platformId)&&this.autofocus()&&setTimeout(()=>{let e=un.getFocusableElements(this.host?.nativeElement);e.length===0&&this.host.nativeElement.focus(),e.length>0&&e[0].focus(),this.focused=!0})}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵdir=CI({type:n,selectors:[[``,`pAutoFocus`,``]],inputs:{autofocus:[1,`pAutoFocus`,`autofocus`]},features:[Yf]})}return n})();var gr=`
    
    .p-badge {
        display: inline-flex;
        border-radius: dt('badge.border.radius');
        align-items: center;
        justify-content: center;
        padding: dt('badge.padding');
        background: dt('badge.primary.background');
        color: dt('badge.primary.color');
        font-size: dt('badge.font.size');
        font-weight: dt('badge.font.weight');
        min-width: dt('badge.min.width');
        height: dt('badge.height');
    }

    .p-badge-dot {
        width: dt('badge.dot.size');
        min-width: dt('badge.dot.size');
        height: dt('badge.dot.size');
        border-radius: 50%;
        padding: 0;
    }

    .p-badge-circle {
        padding: 0;
        border-radius: 50%;
    }

    .p-badge-secondary {
        background: dt('badge.secondary.background');
        color: dt('badge.secondary.color');
    }

    .p-badge-success {
        background: dt('badge.success.background');
        color: dt('badge.success.color');
    }

    .p-badge-info {
        background: dt('badge.info.background');
        color: dt('badge.info.color');
    }

    .p-badge-warn {
        background: dt('badge.warn.background');
        color: dt('badge.warn.color');
    }

    .p-badge-danger {
        background: dt('badge.danger.background');
        color: dt('badge.danger.color');
    }

    .p-badge-contrast {
        background: dt('badge.contrast.background');
        color: dt('badge.contrast.color');
    }

    .p-badge-sm {
        font-size: dt('badge.sm.font.size');
        min-width: dt('badge.sm.min.width');
        height: dt('badge.sm.height');
    }

    .p-badge-lg {
        font-size: dt('badge.lg.font.size');
        min-width: dt('badge.lg.min.width');
        height: dt('badge.lg.height');
    }

    .p-badge-xl {
        font-size: dt('badge.xl.font.size');
        min-width: dt('badge.xl.min.width');
        height: dt('badge.xl.height');
    }

`;var br={root:({instance:n})=>{let i=n.value(),e=n.size(),t=n.badgeSize(),o=n.severity();return[`p-badge p-component`,{"p-badge-circle":j(i)&&String(i).length===1,"p-badge-dot":It(i),"p-badge-sm":e===`small`||t===`small`,"p-badge-lg":e===`large`||t===`large`,"p-badge-xl":e===`xlarge`||t===`xlarge`,"p-badge-info":o===`info`,"p-badge-success":o===`success`,"p-badge-warn":o===`warn`,"p-badge-danger":o===`danger`,"p-badge-secondary":o===`secondary`,"p-badge-contrast":o===`contrast`}]}};var qi=(()=>{class n extends $i$1{name=`badge`;style=gr;classes=br;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var Gi=new _(`BADGE_INSTANCE`);var pn=(()=>{class n extends G{componentName=`Badge`;$pcBadge=C(Gi,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});badgeSize=mL();size=mL();severity=mL();value=mL();badgeDisabled=mL(!1,{transform:wL});_componentStyle=C(qi);displayStyle=sD(()=>this.badgeDisabled()?`none`:null);dataP=sD(()=>{let e=this.value(),t=this.severity(),o=this.size();return this.cn({circle:e!=null&&String(e).length===1,empty:e==null,disabled:this.badgeDisabled(),[t]:t,[o]:o})});onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptms([`host`,`root`]))}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵcmp=II({type:n,selectors:[[`p-badge`]],hostVars:5,hostBindings:function(t,o){t&2&&(np(`data-p`,o.dataP()),xE(o.cx(`root`)),Ep(`display`,o.displayStyle()))},inputs:{badgeSize:[1,`badgeSize`],size:[1,`size`],severity:[1,`severity`],value:[1,`value`],badgeDisabled:[1,`badgeDisabled`]},features:[QE([qi,{provide:Gi,useExisting:n},{provide:X,useExisting:n}]),NI([$]),Yf],decls:1,vars:1,template:function(t,o){t&1&&HE(0),t&2&&Sp(o.value())},dependencies:[Dd],encapsulation:2})}return n})();var Qi=(()=>{class n{static ɵfac=function(t){return new(t||n)};static ɵmod=DI({type:n});static ɵinj=Il({imports:[pn,Dd,Dd]})}return n})();var yr=[`content`];var xr=[`loadingicon`];var _r=[`icon`];var wr=[`*`];function Cr(n,i){n&1&&ap(0)}function Tr(n,i){if(n&1&&op(0,`span`,5),n&2){let e=aE(3);xE(e.cn(e.cx(`loadingIcon`),`pi-spin`,e.$loadingIcon())),rp(`pBind`,e.ptm(`loadingIcon`)),np(`aria-hidden`,!0)}}function Dr(n,i){if(n&1&&(ou(),op(0,`svg`,6)),n&2){let e=aE(3);xE(e.cn(e.cx(`loadingIcon`),e.cx(`spinnerIcon`))),rp(`spin`,!0)(`pBind`,e.ptm(`loadingIcon`)),np(`aria-hidden`,!0)}}function Er(n,i){if(n&1&&qI(0,Tr,1,4,`span`,2)(1,Dr,1,5,`:svg:svg`,4),n&2)GI(aE(2).$loadingIcon()?0:1)}function Sr(n,i){n&1&&ap(0)}function kr(n,i){if(n&1&&Jf(0,Sr,1,0,`ng-container`,7),n&2){let e=aE(2);rp(`ngTemplateOutlet`,e.loadingIconTemplate())(`ngTemplateOutletContext`,e.getLoadingIconTemplateContext())}}function Nr(n,i){if(n&1&&qI(0,Er,2,1)(1,kr,1,2,`ng-container`),n&2)GI(aE().loadingIconTemplate()?1:0)}function Mr(n,i){if(n&1&&op(0,`span`,5),n&2){let e=aE(2);xE(e.cn(e.cx(`icon`),e.$icon())),rp(`pBind`,e.ptm(`icon`)),np(`data-p`,e.dataIconP())}}function Ir(n,i){n&1&&ap(0)}function Lr(n,i){if(n&1&&Jf(0,Ir,1,0,`ng-container`,7),n&2){let e=aE(2);rp(`ngTemplateOutlet`,e.iconTemplate())(`ngTemplateOutletContext`,e.getIconTemplateContext())}}function $r(n,i){if(n&1&&(qI(0,Mr,1,4,`span`,2),qI(1,Lr,1,2,`ng-container`)),n&2){let e=aE();GI(e.$icon()&&!e.iconTemplate()?0:-1),Zy(),GI(!e.icon()&&e.iconTemplate()?1:-1)}}function Ar(n,i){if(n&1&&(ni(0,`span`,5),HE(1),gc()),n&2){let e=aE();xE(e.cx(`label`)),rp(`pBind`,e.ptm(`label`)),np(`aria-hidden`,e.$icon()&&!e.$label())(`data-p`,e.dataLabelP()),Zy(),Sp(e.$label())}}function Or(n,i){if(n&1&&op(0,`p-badge`,3),n&2){let e=aE();rp(`value`,e.$badge())(`severity`,e.$badgeSeverity())(`pt`,e.ptm(`pcBadge`))(`unstyled`,e.unstyled())}}var Fr={root:({instance:n})=>{let i=n.hasIcon(),e=n.label(),t=n.buttonProps(),o=n.loading(),r=n.link(),s=n.severity(),a=n.raised(),d=n.rounded(),c=n.text(),l=n.variant(),b=n.outlined(),x=n.size(),h=n.plain(),M=n.badge(),j=n.hasFluid(),_=n.iconPos();return[`p-button p-component`,{"p-button-icon-only":i&&!e&&!t?.label&&!M,"p-button-vertical":(_===`top`||_===`bottom`)&&e,"p-button-loading":o||t?.loading,"p-button-link":r||t?.link,[`p-button-${s||t?.severity}`]:s||t?.severity,"p-button-raised":a||t?.raised,"p-button-rounded":d||t?.rounded,"p-button-text":c||l===`text`||t?.text||t?.variant===`text`,"p-button-outlined":b||l===`outlined`||t?.outlined||t?.variant===`outlined`,"p-button-sm":x===`small`||t?.size===`small`,"p-button-lg":x===`large`||t?.size===`large`,"p-button-plain":h||t?.plain,"p-button-fluid":j}]},loadingIcon:`p-button-loading-icon`,icon:({instance:n})=>{let i=n.iconPos(),e=n.buttonProps(),t=n.label(),o=n.icon();return[`p-button-icon`,{[`p-button-icon-${i||e?.iconPos}`]:t||e?.label,"p-button-icon-left":(i===`left`||e?.iconPos===`left`)&&t||e?.label,"p-button-icon-right":(i===`right`||e?.iconPos===`right`)&&t||e?.label,"p-button-icon-top":(i===`top`||e?.iconPos===`top`)&&t||e?.label,"p-button-icon-bottom":(i===`bottom`||e?.iconPos===`bottom`)&&t||e?.label},o,e?.icon]},spinnerIcon:({instance:n})=>Object.entries(n.cx(`icon`)).filter(([,i])=>!!i).reduce((i,[e])=>i+` ${e}`,`p-button-loading-icon`),label:`p-button-label`};var Zi=(()=>{class n extends $i$1{name=`button`;style=Hi;classes=Fr;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var Xi=new _(`BUTTON_INSTANCE`);var fn=(()=>{class n extends G{componentName=`Button`;hostName=mL(``);$pcButton=C(Xi,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});_componentStyle=C(Zi);type=mL(`button`);badge=mL();disabled=mL(!1,{transform:wL});raised=mL(!1,{transform:wL});rounded=mL(!1,{transform:wL});text=mL(!1,{transform:wL});plain=mL(!1,{transform:wL});outlined=mL(!1,{transform:wL});link=mL(!1,{transform:wL});tabindex=mL(0,{transform:CL});size=mL();variant=mL();style=mL();styleClass=mL();badgeSeverity=mL(`secondary`);ariaLabel=mL();autofocus=mL(!1,{transform:wL});iconPos=mL(`left`);icon=mL();label=mL();loading=mL(!1,{transform:wL});loadingIcon=mL();severity=mL();buttonProps=mL();fluid=mL(void 0,{transform:wL});iconOnly=mL(!1,{transform:wL});onClick=gL();onFocus=gL();onBlur=gL();contentTemplate=IL(`content`,{descendants:!1});loadingIconTemplate=IL(`loadingicon`,{descendants:!1});iconTemplate=IL(`icon`,{descendants:!1});pcFluid=C(Bi,{optional:!0,host:!0,skipSelf:!0});hasFluid=sD(()=>this.fluid()??!!this.pcFluid);$type=sD(()=>this.type()||this.buttonProps()?.type);$ariaLabel=sD(()=>this.ariaLabel()||this.buttonProps()?.ariaLabel);mergedStyle=sD(()=>this.style()||this.buttonProps()?.style);$disabled=sD(()=>this.disabled()||this.loading()||this.buttonProps()?.disabled);$severity=sD(()=>this.severity()||this.buttonProps()?.severity);$tabindex=sD(()=>this.tabindex()||this.buttonProps()?.tabindex);$autofocus=sD(()=>this.autofocus()||this.buttonProps()?.autofocus);$loading=sD(()=>this.loading()||this.buttonProps()?.loading);$icon=sD(()=>this.icon()||this.buttonProps()?.icon);$label=sD(()=>this.label()||this.buttonProps()?.label);$badge=sD(()=>this.badge()||this.buttonProps()?.badge);$loadingIcon=sD(()=>this.loadingIcon()||this.buttonProps()?.loadingIcon);$badgeSeverity=sD(()=>this.badgeSeverity()||this.buttonProps()?.badgeSeverity);showLabel=sD(()=>!this.contentTemplate()&&this.$label());showBadge=sD(()=>!this.contentTemplate()&&this.$badge());hasIcon=sD(()=>this.$icon()||this.iconTemplate()||this.loadingIcon()||this.loadingIconTemplate());$outlined=sD(()=>this.outlined()||this.variant()===`outlined`||this.buttonProps()?.outlined||this.buttonProps()?.variant===`outlined`);$text=sD(()=>this.text()||this.variant()===`text`||this.buttonProps()?.text||this.buttonProps()?.variant===`text`);$iconOnly=sD(()=>this.iconOnly()||this.hasIcon()&&!this.$label()&&!this.$badge());dataP=sD(()=>this.cn({[this.size()]:this.size(),"icon-only":this.$iconOnly(),loading:this.$loading(),fluid:this.hasFluid(),rounded:this.rounded(),raised:this.raised(),outlined:this.$outlined(),text:this.$text(),link:this.link(),vertical:(this.iconPos()===`top`||this.iconPos()===`bottom`)&&this.$label()}));dataIconP=sD(()=>this.cn({[this.iconPos()]:this.iconPos(),[this.size()]:this.size()}));dataLabelP=sD(()=>this.cn({[this.size()]:this.size(),"icon-only":this.$iconOnly()}));onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptm(`host`))}getLoadingIconTemplateContext(){return{class:this.cx(`loadingIcon`),pt:this.ptm(`loadingIcon`)}}getIconTemplateContext(){return{class:this.cx(`icon`),pt:this.ptm(`icon`)}}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵcmp=II({type:n,selectors:[[`p-button`]],contentQueries:function(t,o,r){t&1&&hp(r,o.contentTemplate,yr,4)(r,o.loadingIconTemplate,xr,4)(r,o.iconTemplate,_r,4),t&2&&hE(3)},inputs:{hostName:[1,`hostName`],type:[1,`type`],badge:[1,`badge`],disabled:[1,`disabled`],raised:[1,`raised`],rounded:[1,`rounded`],text:[1,`text`],plain:[1,`plain`],outlined:[1,`outlined`],link:[1,`link`],tabindex:[1,`tabindex`],size:[1,`size`],variant:[1,`variant`],style:[1,`style`],styleClass:[1,`styleClass`],badgeSeverity:[1,`badgeSeverity`],ariaLabel:[1,`ariaLabel`],autofocus:[1,`autofocus`],iconPos:[1,`iconPos`],icon:[1,`icon`],label:[1,`label`],loading:[1,`loading`],loadingIcon:[1,`loadingIcon`],severity:[1,`severity`],buttonProps:[1,`buttonProps`],fluid:[1,`fluid`],iconOnly:[1,`iconOnly`]},outputs:{onClick:`onClick`,onFocus:`onFocus`,onBlur:`onBlur`},features:[QE([Zi,{provide:Xi,useExisting:n},{provide:X,useExisting:n}]),NI([$]),Yf],ngContentSelectors:wr,decls:7,vars:18,consts:[[`pRipple`,``,3,`click`,`focus`,`blur`,`disabled`,`pAutoFocus`,`pBind`],[4,`ngTemplateOutlet`],[3,`class`,`pBind`],[3,`value`,`severity`,`pt`,`unstyled`],[`data-p-icon`,`spinner`,3,`class`,`spin`,`pBind`],[3,`pBind`],[`data-p-icon`,`spinner`,3,`spin`,`pBind`],[4,`ngTemplateOutlet`,`ngTemplateOutletContext`]],template:function(t,o){t&1&&(lE(),ni(0,`button`,0),up(`click`,function(s){return o.onClick.emit(s)})(`focus`,function(s){return o.onFocus.emit(s)})(`blur`,function(s){return o.onBlur.emit(s)}),uE(1),Jf(2,Cr,1,0,`ng-container`,1),qI(3,Nr,2,1),qI(4,$r,2,2),qI(5,Ar,2,6,`span`,2),qI(6,Or,1,4,`p-badge`,3),gc()),t&2&&(NE(o.mergedStyle()),xE(o.cn(o.cx(`root`),o.styleClass(),o.buttonProps()?.styleClass)),rp(`disabled`,o.$disabled())(`pAutoFocus`,o.$autofocus())(`pBind`,o.ptm(`root`)),np(`type`,o.$type())(`aria-label`,o.$ariaLabel())(`tabindex`,o.$tabindex())(`data-p`,o.dataP())(`data-p-disabled`,o.$disabled())(`data-p-severity`,o.$severity()),Zy(2),rp(`ngTemplateOutlet`,o.contentTemplate()),Zy(),GI(o.$loading()?3:-1),Zy(),GI(o.$loading()?-1:4),Zy(),GI(o.showLabel()?5:-1),Zy(),GI(o.showBadge()?6:-1))},dependencies:[$s$1,pt,Ui,Wi,Qi,pn,$],encapsulation:2})}return n})();var Ji=(()=>{class n{static ɵfac=function(t){return new(t||n)};static ɵmod=DI({type:n});static ɵinj=Il({imports:[fn]})}return n})();var Yi=`
    .p-tabs {
        display: flex;
        flex-direction: column;
    }

    .p-tablist {
        overflow: hidden;
        display: flex;
        position: relative;
        background: dt('tabs.tablist.background');
        border-style: solid;
        border-color: dt('tabs.tablist.border.color');
        border-width: dt('tabs.tablist.border.width');
    }

    .p-tablist-content {
        position: relative;
        display: flex;
        flex-grow: 1;
        min-height: 0;
        overflow-x: auto;
        overflow-y: clip;
        scroll-behavior: smooth;
        scrollbar-width: none;
        overscroll-behavior: contain auto;
    }

    .p-tablist-content::-webkit-scrollbar {
        display: none;
    }

    .p-tablist-nav-button {
        all: unset;
        position: absolute !important;
        flex-shrink: 0;
        inset-block-start: 0;
        z-index: 2;
        height: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
        background: dt('tabs.nav.button.background');
        color: dt('tabs.nav.button.color');
        width: dt('tabs.nav.button.width');
        transition:
            color dt('tabs.transition.duration'),
            outline-color dt('tabs.transition.duration'),
            box-shadow dt('tabs.transition.duration');
        box-shadow: dt('tabs.nav.button.shadow');
        outline-color: transparent;
        cursor: pointer;
    }

    .p-tablist-nav-button:focus-visible {
        z-index: 1;
        box-shadow: dt('tabs.nav.button.focus.ring.shadow');
        outline: dt('tabs.nav.button.focus.ring.width') dt('tabs.nav.button.focus.ring.style') dt('tabs.nav.button.focus.ring.color');
        outline-offset: dt('tabs.nav.button.focus.ring.offset');
    }

    .p-tablist-nav-button:hover {
        color: dt('tabs.nav.button.hover.color');
    }

    .p-tablist-prev-button {
        inset-inline-start: 0;
    }

    .p-tablist-next-button {
        inset-inline-end: 0;
    }

    .p-tablist-prev-button:dir(rtl),
    .p-tablist-next-button:dir(rtl) {
        transform: rotate(180deg);
    }

    .p-tab {
        flex-shrink: 0;
        cursor: pointer;
        user-select: none;
        position: relative;
        border-style: solid;
        white-space: nowrap;
        gap: dt('tabs.tab.gap');
        background: dt('tabs.tab.background');
        border-width: dt('tabs.tab.border.width');
        border-color: dt('tabs.tab.border.color');
        color: dt('tabs.tab.color');
        padding: dt('tabs.tab.padding');
        font-weight: dt('tabs.tab.font.weight');
        font-size: dt('tabs.tab.font.size');
        transition:
            background dt('tabs.transition.duration'),
            border-color dt('tabs.transition.duration'),
            color dt('tabs.transition.duration'),
            outline-color dt('tabs.transition.duration'),
            box-shadow dt('tabs.transition.duration');
        margin: dt('tabs.tab.margin');
        outline-color: transparent;
    }

    .p-tab:not(.p-disabled):focus-visible {
        z-index: 1;
        box-shadow: dt('tabs.tab.focus.ring.shadow');
        outline: dt('tabs.tab.focus.ring.width') dt('tabs.tab.focus.ring.style') dt('tabs.tab.focus.ring.color');
        outline-offset: dt('tabs.tab.focus.ring.offset');
    }

    .p-tab:not(.p-tab-active):not(.p-disabled):hover {
        background: dt('tabs.tab.hover.background');
        border-color: dt('tabs.tab.hover.border.color');
        color: dt('tabs.tab.hover.color');
    }

    .p-tab-active {
        background: dt('tabs.tab.active.background');
        border-color: dt('tabs.tab.active.border.color');
        color: dt('tabs.tab.active.color');
    }

    .p-tabpanels {
        background: dt('tabs.tabpanel.background');
        color: dt('tabs.tabpanel.color');
        padding: dt('tabs.tabpanel.padding');
        outline: 0 none;
    }

    .p-tabpanel:focus-visible {
        box-shadow: dt('tabs.tabpanel.focus.ring.shadow');
        outline: dt('tabs.tabpanel.focus.ring.width') dt('tabs.tabpanel.focus.ring.style') dt('tabs.tabpanel.focus.ring.color');
        outline-offset: dt('tabs.tabpanel.focus.ring.offset');
    }

    .p-tablist-active-bar {
        z-index: 1;
        display: block;
        position: absolute;
        background: dt('tabs.active.bar.background');
        transition: width 250ms cubic-bezier(0.35, 0, 0.25, 1), inset-inline-start 250ms cubic-bezier(0.35, 0, 0.25, 1);
        inset-inline-start: var(--px-active-bar-left);
        inset-block-end: dt('tabs.active.bar.bottom');
        width: var(--px-active-bar-width);
        height: dt('tabs.active.bar.height');
    }
`;var eo={name:`chevron-left`,meta:{tags:[`chevron-left`,`backward`,`previous`,`return`,`left`]},svg:{xmlns:`http://www.w3.org/2000/svg`,width:20,height:20,viewBox:`0 0 20 20`,fill:`none`},nodes:[[`path`,{d:`M11.9697 4.46973C12.2626 4.17684 12.7374 4.17684 13.0303 4.46973C13.3232 4.76262 13.3232 5.23738 13.0303 5.53028L8.56055 10L13.0303 14.4697C13.3232 14.7626 13.3232 15.2374 13.0303 15.5303C12.7374 15.8232 12.2626 15.8232 11.9697 15.5303L6.96973 10.5303C6.67684 10.2374 6.67684 9.76262 6.96973 9.46973L11.9697 4.46973Z`,fill:`currentColor`,key:`es7c15`}]]};var Pr=(n,i)=>i[1].key||n;function Rr(n,i){if(n&1&&(ou(),ip(0,`path`)),n&2){let e=aE().$implicit;np(`d`,e[1].d)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`fill-rule`,e[1].fillRule)(`clip-rule`,e[1].clipRule)(`stroke`,e[1].stroke)(`stroke-width`,e[1].strokeWidth)(`stroke-opacity`,e[1].strokeOpacity)(`opacity`,e[1].opacity)}}function jr(n,i){if(n&1&&(ou(),ip(0,`circle`)),n&2){let e=aE().$implicit;np(`cx`,e[1].cx)(`cy`,e[1].cy)(`r`,e[1].r)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function Hr(n,i){if(n&1&&(ou(),ip(0,`rect`)),n&2){let e=aE().$implicit;np(`x`,e[1].x)(`y`,e[1].y)(`width`,e[1].width)(`height`,e[1].height)(`rx`,e[1].rx)(`ry`,e[1].ry)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function Vr(n,i){if(n&1&&(ou(),ip(0,`line`)),n&2){let e=aE().$implicit;np(`x1`,e[1].x1)(`y1`,e[1].y1)(`x2`,e[1].x2)(`y2`,e[1].y2)(`stroke`,e[1].stroke)(`stroke-opacity`,e[1].strokeOpacity)(`opacity`,e[1].opacity)}}function zr(n,i){if(n&1&&(ou(),ip(0,`polyline`)),n&2){let e=aE().$implicit;np(`points`,e[1].points)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function Wr(n,i){if(n&1&&(ou(),ip(0,`polygon`)),n&2){let e=aE().$implicit;np(`points`,e[1].points)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function Ur(n,i){if(n&1&&(ou(),ip(0,`ellipse`)),n&2){let e=aE().$implicit;np(`cx`,e[1].cx)(`cy`,e[1].cy)(`rx`,e[1].rx)(`ry`,e[1].ry)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function Kr(n,i){if(n&1&&qI(0,Rr,1,9,`:svg:path`)(1,jr,1,6,`:svg:circle`)(2,Hr,1,9,`:svg:rect`)(3,Vr,1,7,`:svg:line`)(4,zr,1,4,`:svg:polyline`)(5,Wr,1,4,`:svg:polygon`)(6,Ur,1,7,`:svg:ellipse`),n&2){let e,t=i.$implicit;GI((e=t[0])===`path`?0:e===`circle`?1:e===`rect`?2:e===`line`?3:e===`polyline`?4:e===`polygon`?5:e===`ellipse`?6:-1)}}var to=(()=>{class n extends Xe{constructor(){super(),this._icon=eo}static ɵfac=function(t){return new(t||n)};static ɵcmp=II({type:n,selectors:[[`svg`,`data-p-icon`,`chevron-left`]],features:[Yf],decls:2,vars:0,template:function(t,o){t&1&&zI(0,Kr,7,1,null,null,Pr),t&2&&QI(o.iconNodes())},encapsulation:2,changeDetection:1})}return n})();var no={name:`chevron-right`,meta:{tags:[`chevron-right`,`forward`,`next`,`right`,`proceed`]},svg:{xmlns:`http://www.w3.org/2000/svg`,width:20,height:20,viewBox:`0 0 20 20`,fill:`none`},nodes:[[`path`,{d:`M6.96973 4.46972C7.26262 4.17683 7.73738 4.17683 8.03028 4.46972L13.0303 9.46972C13.3232 9.76262 13.3232 10.2374 13.0303 10.5303L8.03028 15.5303C7.73738 15.8232 7.26262 15.8232 6.96973 15.5303C6.67684 15.2374 6.67684 14.7626 6.96973 14.4697L11.4395 10L6.96973 5.53027C6.67684 5.23738 6.67684 4.76262 6.96973 4.46972Z`,fill:`currentColor`,key:`cn504p`}]]};var qr=(n,i)=>i[1].key||n;function Gr(n,i){if(n&1&&(ou(),ip(0,`path`)),n&2){let e=aE().$implicit;np(`d`,e[1].d)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`fill-rule`,e[1].fillRule)(`clip-rule`,e[1].clipRule)(`stroke`,e[1].stroke)(`stroke-width`,e[1].strokeWidth)(`stroke-opacity`,e[1].strokeOpacity)(`opacity`,e[1].opacity)}}function Qr(n,i){if(n&1&&(ou(),ip(0,`circle`)),n&2){let e=aE().$implicit;np(`cx`,e[1].cx)(`cy`,e[1].cy)(`r`,e[1].r)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function Zr(n,i){if(n&1&&(ou(),ip(0,`rect`)),n&2){let e=aE().$implicit;np(`x`,e[1].x)(`y`,e[1].y)(`width`,e[1].width)(`height`,e[1].height)(`rx`,e[1].rx)(`ry`,e[1].ry)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function Xr(n,i){if(n&1&&(ou(),ip(0,`line`)),n&2){let e=aE().$implicit;np(`x1`,e[1].x1)(`y1`,e[1].y1)(`x2`,e[1].x2)(`y2`,e[1].y2)(`stroke`,e[1].stroke)(`stroke-opacity`,e[1].strokeOpacity)(`opacity`,e[1].opacity)}}function Jr(n,i){if(n&1&&(ou(),ip(0,`polyline`)),n&2){let e=aE().$implicit;np(`points`,e[1].points)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function Yr(n,i){if(n&1&&(ou(),ip(0,`polygon`)),n&2){let e=aE().$implicit;np(`points`,e[1].points)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function es(n,i){if(n&1&&(ou(),ip(0,`ellipse`)),n&2){let e=aE().$implicit;np(`cx`,e[1].cx)(`cy`,e[1].cy)(`rx`,e[1].rx)(`ry`,e[1].ry)(`fill`,e[1].fill)(`fill-opacity`,e[1].fillOpacity)(`opacity`,e[1].opacity)}}function ts(n,i){if(n&1&&qI(0,Gr,1,9,`:svg:path`)(1,Qr,1,6,`:svg:circle`)(2,Zr,1,9,`:svg:rect`)(3,Xr,1,7,`:svg:line`)(4,Jr,1,4,`:svg:polyline`)(5,Yr,1,4,`:svg:polygon`)(6,es,1,7,`:svg:ellipse`),n&2){let e,t=i.$implicit;GI((e=t[0])===`path`?0:e===`circle`?1:e===`rect`?2:e===`line`?3:e===`polyline`?4:e===`polygon`?5:e===`ellipse`?6:-1)}}var io=(()=>{class n extends Xe{constructor(){super(),this._icon=no}static ɵfac=function(t){return new(t||n)};static ɵcmp=II({type:n,selectors:[[`svg`,`data-p-icon`,`chevron-right`]],features:[Yf],decls:2,vars:0,template:function(t,o){t&1&&zI(0,ts,7,1,null,null,qr),t&2&&QI(o.iconNodes())},encapsulation:2,changeDetection:1})}return n})();var ft=[`*`];var ns=[`previcon`];var is=[`nexticon`];var mo=[`content`];var os=[`prevButton`];var rs=[`nextButton`];var ss=[`inkbar`];function as(n,i){n&1&&ap(0)}function ls(n,i){if(n&1&&Jf(0,as,1,0,`ng-container`,9),n&2)rp(`ngTemplateOutlet`,aE(2).prevIconTemplate())}function ds(n,i){n&1&&(ou(),op(0,`svg`,8))}function cs(n,i){if(n&1){let e=eE();ni(0,`button`,7,2),up(`click`,function(){ql(e);return Gl(aE().onPrevButtonClick())}),qI(2,ls,1,1,`ng-container`)(3,ds,1,0,`:svg:svg`,8),gc()}if(n&2){let e=aE();xE(e.cx(`prevButton`)),rp(`pBind`,e.ptm(`prevButton`)),np(`aria-label`,e.prevButtonAriaLabel)(`tabindex`,e.tabindex())(`data-pc-group-section`,`navigator`),Zy(2),GI(e.prevIconTemplate()?2:3)}}function us(n,i){n&1&&ap(0)}function ps(n,i){if(n&1&&Jf(0,us,1,0,`ng-container`,9),n&2)rp(`ngTemplateOutlet`,aE(2).nextIconTemplate())}function fs(n,i){n&1&&(ou(),op(0,`svg`,10))}function hs(n,i){if(n&1){let e=eE();ni(0,`button`,7,3),up(`click`,function(){ql(e);return Gl(aE().onNextButtonClick())}),qI(2,ps,1,1,`ng-container`)(3,fs,1,0,`:svg:svg`,10),gc()}if(n&2){let e=aE();xE(e.cx(`nextButton`)),rp(`pBind`,e.ptm(`nextButton`)),np(`aria-label`,e.nextButtonAriaLabel)(`tabindex`,e.tabindex())(`data-pc-group-section`,`navigator`),Zy(2),GI(e.nextIconTemplate()?2:3)}}function ms(n,i){n&1&&uE(0)}function gs(n,i){n&1&&ap(0)}function bs(n,i){if(n&1&&Jf(0,gs,1,0,`ng-container`,1),n&2){let e=aE(),t=gE(1);rp(`ngTemplateOutlet`,e.content()?e.content():t)}}var vs={root:`p-tabs p-component`};var oo=(()=>{class n extends $i$1{name=`tabs`;style=Yi;classes=vs;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var ro=new _(`TABS_INSTANCE`);var $e=(()=>{class n extends G{componentName=`Tabs`;$pcTabs=C(ro,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});value=yL(void 0);scrollable=mL(!1,{transform:wL});lazy=mL(!1,{transform:wL});selectOnFocus=mL(!1,{transform:wL});showNavigators=mL(!0,{transform:wL});tabindex=mL(0,{transform:CL});scrollStrategy=mL(`nearest`);id=_o$1(Qe(`pn_id_`));_componentStyle=C(oo);onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptms([`host`,`root`]))}updateValue(e){this.value.update(()=>e)}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵcmp=II({type:n,selectors:[[`p-tabs`]],hostVars:3,hostBindings:function(t,o){t&2&&(np(`id`,o.id()),xE(o.cx(`root`)))},inputs:{value:[1,`value`],scrollable:[1,`scrollable`],lazy:[1,`lazy`],selectOnFocus:[1,`selectOnFocus`],showNavigators:[1,`showNavigators`],tabindex:[1,`tabindex`],scrollStrategy:[1,`scrollStrategy`]},outputs:{value:`valueChange`},features:[QE([oo,{provide:ro,useExisting:n},{provide:X,useExisting:n}]),NI([$]),Yf],ngContentSelectors:ft,decls:1,vars:0,template:function(t,o){t&1&&(lE(),uE(0))},dependencies:[ve],encapsulation:2})}return n})();var ys={root:({instance:n})=>[`p-tab`,{"p-tab-active":n.active(),"p-disabled":n.disabled()}]};var so=(()=>{class n extends $i$1{name=`tab`;classes=ys;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var xs={root:`p-tablist`,content:`p-tablist-content`,activeBar:`p-tablist-active-bar`,prevButton:`p-tablist-prev-button p-tablist-nav-button`,nextButton:`p-tablist-next-button p-tablist-nav-button`};var ao=(()=>{class n extends $i$1{name=`tablist`;classes=xs;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var lo=new _(`TABLIST_INSTANCE`);var Je=(()=>{class n extends G{componentName=`TabList`;$pcTabList=C(lo,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});prevIconTemplate=IL(`previcon`,{descendants:!1});nextIconTemplate=IL(`nexticon`,{descendants:!1});content=vL(`content`);prevButton=vL(`prevButton`);nextButton=vL(`nextButton`);inkbar=vL(`inkbar`);pcTabs=C(no$1(()=>$e));isPrevButtonEnabled=_o$1(!1);isNextButtonEnabled=_o$1(!1);resizeObserver;showNavigators=sD(()=>this.pcTabs.showNavigators());tabindex=sD(()=>this.pcTabs.tabindex());scrollable=sD(()=>this.pcTabs.scrollable());_componentStyle=C(ao);constructor(){super(),hu(()=>{this.pcTabs.value(),fc(this.platformId)&&setTimeout(()=>{this.updateInkBar(),this.scrollToActiveTab()})})}get prevButtonAriaLabel(){return this.config?.translation?.aria?.previous}get nextButtonAriaLabel(){return this.config?.translation?.aria?.next}onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptms([`host`,`root`]))}onAfterViewInit(){this.showNavigators()&&fc(this.platformId)&&(this.updateButtonState(),this.bindResizeObserver())}onDestroy(){this.unbindResizeObserver()}onScroll(e){this.showNavigators()&&this.updateButtonState(),e.preventDefault()}onPrevButtonClick(){let e=this.content()?.nativeElement;if(!e)return;let t=ad(e),o=Math.abs(e.scrollLeft)-t,r=o<=0?0:o;e.scrollLeft=Xl(e)?-1*r:r}onNextButtonClick(){let e=this.content()?.nativeElement;if(!e)return;let t=ad(e)-this.getVisibleButtonWidths(),o=e.scrollLeft+t,r=e.scrollWidth-t,s=o>=r?r:o;e.scrollLeft=Xl(e)?-1*s:s}updateButtonState(){let e=this.content()?.nativeElement,t=this.el?.nativeElement;if(!e)return;let{scrollWidth:o,offsetWidth:r}=e,s=Math.abs(e.scrollLeft),a=ad(e);this.isPrevButtonEnabled.set(s!==0),this.isNextButtonEnabled.set(t.offsetWidth>=r&&Math.abs(s-o+a)>1)}updateInkBar(){let e=this.content()?.nativeElement,t=this.inkbar()?.nativeElement;if(!e||!t)return;let o=td(e,`[data-pc-name="tab"][data-p-active="true"]`);o&&(t.style.setProperty(`--px-active-bar-width`,o.offsetWidth+`px`),t.style.setProperty(`--px-active-bar-height`,o.offsetHeight+`px`),t.style.setProperty(`--px-active-bar-left`,o.offsetLeft+`px`),t.style.setProperty(`--px-active-bar-top`,o.offsetTop+`px`))}scrollToActiveTab(){let e=this.content()?.nativeElement,t=this.pcTabs.scrollStrategy();if(!e||t===!1)return;let o=td(e,`[data-pc-name="tab"][data-p-active="true"]`);if(!o)return;let r=e.clientWidth,s=Math.abs(e.scrollLeft),a=o.offsetLeft,d=o.offsetWidth,c=a+d,l;if(t===`center`)l=a-(r-d)/2;else{let h=r*.1;if(a<s+h)l=a-h;else if(c>s+r-h)l=c-r+h;else return}let b=e.scrollWidth-r,x=Math.max(0,Math.min(l,b));e.scrollTo({left:Xl(e)?-x:x,behavior:`smooth`})}getVisibleButtonWidths(){return[this.prevButton()?.nativeElement,this.nextButton()?.nativeElement].reduce((o,r)=>r?o+ad(r):o,0)}bindResizeObserver(){this.resizeObserver=new ResizeObserver(()=>this.updateButtonState()),this.resizeObserver.observe(this.el.nativeElement)}unbindResizeObserver(){this.resizeObserver&&(this.resizeObserver.unobserve(this.el.nativeElement),this.resizeObserver=null)}static ɵfac=function(t){return new(t||n)};static ɵcmp=II({type:n,selectors:[[`p-tablist`]],contentQueries:function(t,o,r){t&1&&hp(r,o.prevIconTemplate,ns,4)(r,o.nextIconTemplate,is,4),t&2&&hE(2)},viewQuery:function(t,o){t&1&&gp(o.content,mo,5)(o.prevButton,os,5)(o.nextButton,rs,5)(o.inkbar,ss,5),t&2&&hE(4)},hostVars:2,hostBindings:function(t,o){t&2&&xE(o.cx(`root`))},features:[QE([ao,{provide:lo,useExisting:n},{provide:X,useExisting:n}]),NI([$]),Yf],ngContentSelectors:ft,decls:7,vars:8,consts:[[`content`,``],[`inkbar`,``],[`prevButton`,``],[`nextButton`,``],[`type`,`button`,`pRipple`,``,3,`pBind`,`class`],[`role`,`tablist`,3,`scroll`,`pBind`],[`role`,`presentation`,3,`pBind`],[`type`,`button`,`pRipple`,``,3,`click`,`pBind`],[`data-p-icon`,`chevron-left`],[4,`ngTemplateOutlet`],[`data-p-icon`,`chevron-right`]],template:function(t,o){t&1&&(lE(),qI(0,cs,4,7,`button`,4),ni(1,`div`,5,0),up(`scroll`,function(s){return o.onScroll(s)}),uE(3),op(4,`span`,6,1),gc(),qI(6,hs,4,7,`button`,4)),t&2&&(GI(o.showNavigators()&&o.isPrevButtonEnabled()?0:-1),Zy(),xE(o.cx(`content`)),rp(`pBind`,o.ptm(`content`)),Zy(3),xE(o.cx(`activeBar`)),rp(`pBind`,o.ptm(`activeBar`)),Zy(2),GI(o.showNavigators()&&o.isNextButtonEnabled()?6:-1))},dependencies:[$s$1,to,io,ji,pt,Dd,ve,$],encapsulation:2})}return n})();var co=new _(`TAB_INSTANCE`);var ht=(()=>{class n extends G{componentName=`Tab`;$pcTab=C(co,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});value=yL();disabled=mL(!1,{transform:wL});pcTabs=C(no$1(()=>$e));pcTabList=C(no$1(()=>Je));el=C(dr$1);_componentStyle=C(so);ripple=sD(()=>this.config.ripple());id=sD(()=>`${this.pcTabs.id()}_tab_${this.value()}`);ariaControls=sD(()=>`${this.pcTabs.id()}_tabpanel_${this.value()}`);active=sD(()=>jl(this.pcTabs.value(),this.value()));tabindex=sD(()=>this.disabled()?-1:this.active()?this.pcTabs.tabindex():-1);mutationObserver;onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptms([`host`,`root`]))}onFocus(e){this.disabled()||this.pcTabs.selectOnFocus()&&this.changeActiveValue()}onClick(e){this.disabled()||this.changeActiveValue()}onKeyDown(e){switch(e.code){case`ArrowRight`:this.onArrowRightKey(e);break;case`ArrowLeft`:this.onArrowLeftKey(e);break;case`Home`:this.onHomeKey(e);break;case`End`:this.onEndKey(e);break;case`PageDown`:this.onPageDownKey(e);break;case`PageUp`:this.onPageUpKey(e);break;case`Enter`:case`NumpadEnter`:case`Space`:this.onEnterKey(e);break;default:break}e.stopPropagation()}onAfterViewInit(){this.bindMutationObserver()}onArrowRightKey(e){let t=this.findNextTab(e.currentTarget);t?this.changeFocusedTab(t):this.onHomeKey(e),e.preventDefault()}onArrowLeftKey(e){let t=this.findPrevTab(e.currentTarget);t?this.changeFocusedTab(t):this.onEndKey(e),e.preventDefault()}onHomeKey(e){let t=this.findFirstTab();this.changeFocusedTab(t),e.preventDefault()}onEndKey(e){let t=this.findLastTab();this.changeFocusedTab(t),e.preventDefault()}onPageDownKey(e){this.scrollInView(this.findLastTab()),e.preventDefault()}onPageUpKey(e){this.scrollInView(this.findFirstTab()),e.preventDefault()}onEnterKey(e){this.disabled()||this.changeActiveValue(),e.preventDefault()}findNextTab(e,t=!1){let o=t?e:e?.nextElementSibling;return o?rd(o,`data-p-disabled`)||rd(o,`data-pc-section`)===`activebar`?this.findNextTab(o):o:null}findPrevTab(e,t=!1){let o=t?e:e?.previousElementSibling;return o?rd(o,`data-p-disabled`)||rd(o,`data-pc-section`)===`activebar`?this.findPrevTab(o):o:null}findFirstTab(){return this.findNextTab(this.pcTabList?.content()?.nativeElement?.firstElementChild,!0)}findLastTab(){return this.findPrevTab(this.pcTabList?.content()?.nativeElement?.lastElementChild,!0)}changeActiveValue(){this.pcTabs.updateValue(this.value())}changeFocusedTab(e){e&&nd(e),this.scrollInView(e)}scrollInView(e){e?.scrollIntoView?.({block:`nearest`})}bindMutationObserver(){fc(this.platformId)&&(this.mutationObserver=new MutationObserver(e=>{e.forEach(()=>{this.active()&&this.pcTabList?.updateInkBar()})}),this.mutationObserver.observe(this.el.nativeElement,{childList:!0,characterData:!0,subtree:!0}))}unbindMutationObserver(){this.mutationObserver?.disconnect()}onDestroy(){this.mutationObserver&&this.unbindMutationObserver()}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵcmp=II({type:n,selectors:[[`p-tab`]],hostVars:10,hostBindings:function(t,o){t&1&&up(`focus`,function(s){return o.onFocus(s)})(`click`,function(s){return o.onClick(s)})(`keydown`,function(s){return o.onKeyDown(s)}),t&2&&(np(`id`,o.id())(`aria-controls`,o.ariaControls())(`role`,`tab`)(`aria-selected`,o.active())(`aria-disabled`,o.disabled())(`data-p-disabled`,o.disabled())(`data-p-active`,o.active())(`tabindex`,o.tabindex()),xE(o.cx(`root`)))},inputs:{value:[1,`value`],disabled:[1,`disabled`]},outputs:{value:`valueChange`},features:[QE([so,{provide:co,useExisting:n},{provide:X,useExisting:n}]),NI([pt,$]),Yf],ngContentSelectors:ft,decls:1,vars:0,template:function(t,o){t&1&&(lE(),uE(0))},dependencies:[Dd,ve],encapsulation:2})}return n})();var _s={root:({instance:n})=>[`p-tabpanel`,{"p-tabpanel-active":n.active()}]};var uo=(()=>{class n extends $i$1{name=`tabpanel`;classes=_s;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var po=new _(`TABPANEL_INSTANCE`);var ws=(()=>{class n extends G{componentName=`TabPanel`;$pcTabPanel=C(po,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});pcTabs=C(no$1(()=>$e));lazy=mL(!1,{transform:wL});value=yL(void 0);content=IL(`content`,{descendants:!1});id=sD(()=>`${this.pcTabs.id()}_tabpanel_${this.value()}`);ariaLabelledby=sD(()=>`${this.pcTabs.id()}_tab_${this.value()}`);active=sD(()=>jl(this.pcTabs.value(),this.value()));isLazyEnabled=sD(()=>this.pcTabs.lazy()||this.lazy());hasBeenRendered=!1;shouldRender=sD(()=>!this.isLazyEnabled()||this.hasBeenRendered?!0:this.active()?(this.hasBeenRendered=!0,!0):!1);_componentStyle=C(uo);onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptms([`host`,`root`]))}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵcmp=II({type:n,selectors:[[`p-tabpanel`]],contentQueries:function(t,o,r){t&1&&hp(r,o.content,mo,4),t&2&&hE()},hostVars:7,hostBindings:function(t,o){t&2&&(cp(`hidden`,!o.active()),np(`id`,o.id())(`role`,`tabpanel`)(`aria-labelledby`,o.ariaLabelledby())(`data-p-active`,o.active()),xE(o.cx(`root`)))},inputs:{lazy:[1,`lazy`],value:[1,`value`]},outputs:{value:`valueChange`},features:[QE([uo,{provide:po,useExisting:n},{provide:X,useExisting:n}]),NI([$]),Yf],ngContentSelectors:ft,decls:3,vars:1,consts:[[`defaultContent`,``],[4,`ngTemplateOutlet`]],template:function(t,o){t&1&&(lE(),Jf(0,ms,1,0,`ng-template`,null,0,nD),qI(2,bs,1,1,`ng-container`)),t&2&&(Zy(2),GI(o.shouldRender()?2:-1))},dependencies:[$s$1,ve],encapsulation:2})}return n})();var Cs={root:`p-tabpanels`};var fo=(()=>{class n extends $i$1{name=`tabpanels`;classes=Cs;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var ho=new _(`TABPANELS_INSTANCE`);var Ts=(()=>{class n extends G{componentName=`TabPanels`;$pcTabPanels=C(ho,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});_componentStyle=C(fo);onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptms([`host`,`root`]))}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵcmp=II({type:n,selectors:[[`p-tabpanels`]],hostVars:3,hostBindings:function(t,o){t&2&&(np(`role`,`presentation`),xE(o.cx(`root`)))},features:[QE([fo,{provide:ho,useExisting:n},{provide:X,useExisting:n}]),NI([$]),Yf],ngContentSelectors:ft,decls:1,vars:0,template:function(t,o){t&1&&(lE(),uE(0))},dependencies:[ve],encapsulation:2})}return n})();var zt=(()=>{class n{static ɵfac=function(t){return new(t||n)};static ɵmod=DI({type:n});static ɵinj=Il({imports:[$e,Ts,ws,Je,ht,ve,ve]})}return n})();var bo=`
    .p-tag {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        background: dt('tag.primary.background');
        color: dt('tag.primary.color');
        font-size: dt('tag.font.size');
        font-weight: dt('tag.font.weight');
        padding: dt('tag.padding');
        border-radius: dt('tag.border.radius');
        gap: dt('tag.gap');
    }

    .p-tag-icon {
        font-size: dt('tag.icon.size');
        width: dt('tag.icon.size');
        height: dt('tag.icon.size');
    }

    .p-tag-rounded {
        border-radius: dt('tag.rounded.border.radius');
    }

    .p-tag-success {
        background: dt('tag.success.background');
        color: dt('tag.success.color');
    }

    .p-tag-info {
        background: dt('tag.info.background');
        color: dt('tag.info.color');
    }

    .p-tag-warn {
        background: dt('tag.warn.background');
        color: dt('tag.warn.color');
    }

    .p-tag-danger {
        background: dt('tag.danger.background');
        color: dt('tag.danger.color');
    }

    .p-tag-secondary {
        background: dt('tag.secondary.background');
        color: dt('tag.secondary.color');
    }

    .p-tag-contrast {
        background: dt('tag.contrast.background');
        color: dt('tag.contrast.color');
    }
`;var Ds=[`icon`];var Es=[`*`];function Ss(n,i){if(n&1&&op(0,`span`,1),n&2){let e=aE(2);xE(e.cn(e.cx(`icon`),e.icon())),rp(`pBind`,e.ptm(`icon`))}}function ks(n,i){if(n&1&&qI(0,Ss,1,3,`span`,0),n&2)GI(aE().icon()?0:-1)}function Ns(n,i){if(n&1&&(ni(0,`span`,1),ap(1,2),gc()),n&2){let e=aE();xE(e.cx(`icon`)),rp(`pBind`,e.ptm(`icon`)),Zy(),rp(`ngTemplateOutlet`,e.iconTemplate())}}var Ms={root:({instance:n})=>{let i=n.severity(),e=n.rounded();return[`p-tag p-component`,{"p-tag-info":i===`info`,"p-tag-success":i===`success`,"p-tag-warn":i===`warn`,"p-tag-danger":i===`danger`,"p-tag-secondary":i===`secondary`,"p-tag-contrast":i===`contrast`,"p-tag-rounded":e}]},icon:`p-tag-icon`,label:`p-tag-label`};var vo=(()=>{class n extends $i$1{name=`tag`;style=bo;classes=Ms;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var yo=new _(`TAG_INSTANCE`);var mt=(()=>{class n extends G{componentName=`Tag`;$pcTag=C(yo,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});severity=mL();value=mL();icon=mL();rounded=mL(!1,{transform:wL});iconTemplate=IL(`icon`,{descendants:!1});_componentStyle=C(vo);dataP=sD(()=>{let e=this.severity(),t=this.rounded();return this.cn({rounded:t,[e]:e})});onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptms([`host`,`root`]))}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵcmp=II({type:n,selectors:[[`p-tag`]],contentQueries:function(t,o,r){t&1&&hp(r,o.iconTemplate,Ds,4),t&2&&hE()},hostVars:3,hostBindings:function(t,o){t&2&&(np(`data-p`,o.dataP()),xE(o.cx(`root`)))},inputs:{severity:[1,`severity`],value:[1,`value`],icon:[1,`icon`],rounded:[1,`rounded`]},features:[QE([vo,{provide:yo,useExisting:n},{provide:X,useExisting:n}]),NI([$]),Yf],ngContentSelectors:Es,decls:5,vars:5,consts:[[3,`class`,`pBind`],[3,`pBind`],[3,`ngTemplateOutlet`]],template:function(t,o){t&1&&(lE(),uE(0),qI(1,ks,1,1)(2,Ns,2,4,`span`,0),ni(3,`span`,1),HE(4),gc()),t&2&&(Zy(),GI(o.iconTemplate()?2:1),Zy(2),xE(o.cx(`label`)),rp(`pBind`,o.ptm(`label`)),Zy(),Sp(o.value()))},dependencies:[$s$1,Dd,$],encapsulation:2})}return n})();var Wt=(()=>{class n{static ɵfac=function(t){return new(t||n)};static ɵmod=DI({type:n});static ɵinj=Il({imports:[mt,Dd,Dd]})}return n})();var Is=5e3;var Ut=class n{http=C(jo$1);_apps=_o$1([]);_loaded=_o$1(!1);_activeName=_o$1(null);apps=sD(()=>{let i=this._apps(),e=this._activeName();return e&&!i.some(t=>t.name===e)?[...i,{name:e,aggregateTypes:[],nodes:[]}].sort((t,o)=>t.name.localeCompare(o.name)):i});noApps=sD(()=>this._loaded()&&this.apps().length===0);activeApp=sD(()=>this.apps().find(i=>i.name===this._activeName())??null,{equal:(i,e)=>i?.name===e?.name});baseUrl=sD(()=>`/api/apps/${encodeURIComponent(this.activeApp()?.name??``)}`);load(){let i=()=>document.visibilityState===`visible`;return new Promise(e=>{qh(Uh(0,Is),ji$1(document,`visibilitychange`)).pipe(zr$1((t,o)=>o===0||i()),fl(()=>this.http.get(`/api/apps`).pipe(Vi$1(()=>Ch(null))))).subscribe(t=>{t&&this.update(t),this._loaded.set(!0),e()})})}setActiveApp(i){this._activeName.set(i.name)}update(i){this._apps.set(i),this._activeName()===null&&i.length>0&&this._activeName.set(i[0].name)}static ɵfac=function(e){return new(e||n)};static ɵprov=pe({token:n,factory:n.ɵfac,providedIn:`root`})};var Ye=class n{http=C(jo$1);backend=C(Ut);getEvents(i,e,t,o=50){let r=new Ne().set(`limit`,o);return t!=null&&(r=r.set(`cursor`,t)),this.http.get(`${this.aggregateUrl(i,e)}/events`,{params:r})}getEventDetail(i,e,t){return this.http.get(`${this.aggregateUrl(i,e)}/events/${t}`)}getEventsOfCommand(i,e){return this.http.get(`${this.aggregateUrl(i,e.aggregateId)}/commands/${encodeURIComponent(e.id)}/events`)}getCommands(i,e,t=500){let o=new Ne().set(`limit`,t);return this.http.get(`${this.aggregateUrl(i,e)}/commands`,{params:o})}aggregateUrl(i,e){return`${this.backend.baseUrl()}/aggregates/${encodeURIComponent(i)}/${encodeURIComponent(e)}`}retryCommand(i){return this.http.post(`${this.backend.baseUrl()}/commands/retry`,i)}static ɵfac=function(e){return new(e||n)};static ɵprov=pe({token:n,factory:n.ɵfac,providedIn:`root`})};function _o(n){n||(n=C(Ce));let i=new b(e=>{if(n.destroyed){e.next();return}return n.onDestroy(e.next.bind(e))});return e=>e.pipe(Kh(i))}function Ls(){let n=[],i=(r,s)=>{let a=n.length>0?n[n.length-1]:{key:r,value:s},d=a.value+(a.key===r?0:s)+2;return n.push({key:r,value:d}),d},e=r=>{n=n.filter(s=>s.value!==r)},t=()=>n.length>0?n[n.length-1].value:0,o=r=>r&&parseInt(r.style.zIndex,10)||0;return{get:o,set:(r,s,a)=>{s&&(s.style.zIndex=String(i(r,a)))},clear:r=>{r&&(e(o(r)),r.style.zIndex=``)},getCurrent:()=>t(),generateZIndex:i,revertZIndex:e}}var Kt=Ls();var wo=`
    .p-tooltip {
        position: absolute;
        display: none;
        max-width: dt('tooltip.max.width');
    }

    .p-tooltip-right,
    .p-tooltip-left {
        padding: 0 dt('tooltip.gutter');
    }

    .p-tooltip-top,
    .p-tooltip-bottom {
        padding: dt('tooltip.gutter') 0;
    }

    .p-tooltip-text {
        white-space: pre-line;
        word-break: break-word;
        background: dt('tooltip.background');
        color: dt('tooltip.color');
        padding: dt('tooltip.padding');
        box-shadow: dt('tooltip.shadow');
        border-radius: dt('tooltip.border.radius');
        font-weight: dt('tooltip.font.weight');
        font-size: dt('tooltip.font.size');
    }

    .p-tooltip-arrow {
        position: absolute;
        width: 0;
        height: 0;
        border-color: transparent;
        border-style: solid;
    }

    .p-tooltip-right .p-tooltip-arrow {
        margin-top: calc(-1 * dt('tooltip.gutter'));
        border-width: dt('tooltip.gutter') dt('tooltip.gutter') dt('tooltip.gutter') 0;
        border-right-color: dt('tooltip.background');
    }

    .p-tooltip-left .p-tooltip-arrow {
        margin-top: calc(-1 * dt('tooltip.gutter'));
        border-width: dt('tooltip.gutter') 0 dt('tooltip.gutter') dt('tooltip.gutter');
        border-left-color: dt('tooltip.background');
    }

    .p-tooltip-top .p-tooltip-arrow {
        margin-left: calc(-1 * dt('tooltip.gutter'));
        border-width: dt('tooltip.gutter') dt('tooltip.gutter') 0 dt('tooltip.gutter');
        border-top-color: dt('tooltip.background');
        border-bottom-color: dt('tooltip.background');
    }

    .p-tooltip-bottom .p-tooltip-arrow {
        margin-left: calc(-1 * dt('tooltip.gutter'));
        border-width: 0 dt('tooltip.gutter') dt('tooltip.gutter') dt('tooltip.gutter');
        border-top-color: dt('tooltip.background');
        border-bottom-color: dt('tooltip.background');
    }
`;var $s={root:`p-tooltip p-component`,arrow:`p-tooltip-arrow`,text:`p-tooltip-text`};var Co=(()=>{class n extends $i$1{name=`tooltip`;style=wo;classes=$s;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var To=new _(`TOOLTIP_INSTANCE`);var Do=(()=>{class n extends G{componentName=`Tooltip`;$pcTooltip=C(To,{optional:!0,skipSelf:!0})??void 0;tooltipPosition=mL();tooltipEvent=mL(`hover`);positionStyle=mL();tooltipStyleClass=mL();tooltipZIndex=mL();escape=mL(!0,{transform:wL});showDelay=mL(void 0,{transform:CL});hideDelay=mL(void 0,{transform:CL});life=mL(void 0,{transform:CL});positionTop=mL(void 0,{transform:CL});positionLeft=mL(void 0,{transform:CL});autoHide=mL(!0,{transform:wL});fitContent=mL(!0,{transform:wL});hideOnEscape=mL(!0,{transform:wL});showOnEllipsis=mL(!1,{transform:wL});content=mL(void 0,{alias:`pTooltip`});tooltipDisabled=mL(!1,{transform:wL});tooltipOptions=mL();appendTo=mL(void 0);$appendTo=sD(()=>this.appendTo()||this.config.overlayAppendTo());tooltipId=Qe(`pn_id_`)+`_tooltip`;_tooltipOptions=sD(()=>W(G$1({tooltipLabel:this.content(),tooltipPosition:this.tooltipPosition()??`right`,tooltipEvent:this.tooltipEvent(),appendTo:this.appendTo()??`body`,positionStyle:this.positionStyle(),tooltipStyleClass:this.tooltipStyleClass(),tooltipZIndex:this.tooltipZIndex()??`auto`,escape:this.escape(),showDelay:this.showDelay(),hideDelay:this.hideDelay(),life:this.life(),positionTop:this.positionTop()??0,positionLeft:this.positionLeft()??0,autoHide:this.autoHide(),hideOnEscape:this.hideOnEscape(),showOnEllipsis:this.showOnEllipsis(),disabled:this.tooltipDisabled()},this.tooltipOptions()),{id:this.tooltipId}));container=null;styleClass;tooltipText=null;rootPTClasses=``;showTimeout=null;hideTimeout=null;active;mouseEnterListener;mouseLeaveListener;clickListener;focusListener;blurListener;touchStartListener;touchEndListener;containerMouseleaveListener;documentTouchListener;documentEscapeListener;scrollHandler=null;resizeListener=null;_componentStyle=C(Co);pTooltipPT=mL();pTooltipUnstyled=mL();viewContainer=C(hi);constructor(){super(),hu(()=>{let e=this.pTooltipPT();e&&this.directivePT.set(e)}),hu(()=>{this.pTooltipUnstyled()&&this.directiveUnstyled.set(this.pTooltipUnstyled())}),hu(()=>{let e=this.content();Up(()=>{this.active&&(e?this.container&&this.container.offsetParent?(this.updateText(),this.align()):this.show():this.hide())})}),hu(()=>{let e=this.tooltipDisabled();Up(()=>{e&&this.deactivate()})}),hu(()=>{let e=this.tooltipOptions();Up(()=>{e&&(this.deactivate(),this.active&&(this.getOption(`tooltipLabel`)?this.container&&this.container.offsetParent?(this.updateText(),this.align()):this.show():this.hide()))})})}onAfterViewInit(){if(fc(this.platformId)){let e=this.getOption(`tooltipEvent`);if((e===`hover`||e===`both`)&&(this.mouseEnterListener=this.onMouseEnter.bind(this),this.mouseLeaveListener=this.onMouseLeave.bind(this),this.clickListener=this.onInputClick.bind(this),this.el.nativeElement.addEventListener(`mouseenter`,this.mouseEnterListener),this.el.nativeElement.addEventListener(`click`,this.clickListener),this.el.nativeElement.addEventListener(`mouseleave`,this.mouseLeaveListener),this.touchStartListener=this.onTouchStart.bind(this),this.touchEndListener=this.onTouchEnd.bind(this),this.el.nativeElement.addEventListener(`touchstart`,this.touchStartListener,{passive:!0}),this.el.nativeElement.addEventListener(`touchend`,this.touchEndListener,{passive:!0})),e===`focus`||e===`both`){this.focusListener=this.onFocus.bind(this),this.blurListener=this.onBlur.bind(this);let t=this.el.nativeElement.querySelector(`.p-component`);t||(t=this.getTarget(this.el.nativeElement)),t.addEventListener(`focus`,this.focusListener),t.addEventListener(`blur`,this.blurListener)}}}isAutoHide(){return this.getOption(`autoHide`)}onMouseEnter(e){!this.container&&!this.showTimeout&&this.activate()}onMouseLeave(e){this.isAutoHide()?this.deactivate():Ko$1(e.relatedTarget,`p-tooltip`)||Ko$1(e.relatedTarget,`p-tooltip-text`)||Ko$1(e.relatedTarget,`p-tooltip-arrow`)||this.deactivate()}onTouchStart(e){!this.container&&!this.showTimeout&&(this.activate(),this.isAutoHide()||this.bindDocumentTouchListener())}onTouchEnd(e){this.isAutoHide()&&this.deactivate()}bindDocumentTouchListener(){this.documentTouchListener||(this.documentTouchListener=this.renderer.listen(`document`,`touchstart`,e=>{let t=e.target;this.container&&!this.container.contains(t)&&!this.el.nativeElement.contains(t)&&(this.deactivate(),this.unbindDocumentTouchListener())}))}unbindDocumentTouchListener(){this.documentTouchListener&&(this.documentTouchListener(),this.documentTouchListener=null)}onFocus(e){this.activate()}onBlur(e){this.deactivate()}onInputClick(e){this.deactivate()}hasEllipsis(){let e=this.el.nativeElement;return e.offsetWidth<e.scrollWidth||e.offsetHeight<e.scrollHeight}activate(){if(this.active||this.getOption(`showOnEllipsis`)&&!this.hasEllipsis())return;this.active=!0,this.clearHideTimeout();let e=this.getOption(`showDelay`);e?this.showTimeout=setTimeout(()=>{this.show()},e):this.show();let t=this.getOption(`life`);if(t){let o=e?t+e:t;this.hideTimeout=setTimeout(()=>{this.hide()},o)}this.getOption(`hideOnEscape`)&&(this.documentEscapeListener=this.renderer.listen(`document`,`keydown.escape`,()=>{this.deactivate(),this.documentEscapeListener?.()}))}deactivate(){this.active=!1,this.clearShowTimeout();let e=this.getOption(`hideDelay`);e?(this.clearHideTimeout(),this.hideTimeout=setTimeout(()=>{this.hide()},e)):this.hide(),this.documentEscapeListener&&this.documentEscapeListener()}create(){this.container&&(this.clearHideTimeout(),this.remove());let e=Ql(`div`,{class:this.cx(`root`),"p-bind":this.ptm(`root`),"data-pc-section":`root`}),t=Ql(`div`,{class:this.cx(`arrow`),"p-bind":this.ptm(`arrow`),"data-pc-section":`arrow`}),o=Ql(`div`,{class:this.cx(`text`),"p-bind":this.ptm(`text`),"data-pc-section":`text`});e.setAttribute(`role`,`tooltip`),e.appendChild(t),this.container=e,this.tooltipText=o,this.updateText(),this.getOption(`positionStyle`)&&(e.style.position=this.getOption(`positionStyle`)),e.appendChild(o),this.getOption(`appendTo`)===`body`?document.body.appendChild(e):this.getOption(`appendTo`)===`target`?Jl(e,this.el.nativeElement):Jl(this.getOption(`appendTo`),e),e.style.display=`none`,this.fitContent()&&(e.style.width=`fit-content`),this.isAutoHide()?e.style.pointerEvents=`none`:(e.style.pointerEvents=`unset`,this.bindContainerMouseleaveListener())}bindContainerMouseleaveListener(){!this.containerMouseleaveListener&&this.container&&(this.containerMouseleaveListener=this.renderer.listen(this.container,`mouseleave`,()=>{this.deactivate()}))}unbindContainerMouseleaveListener(){this.containerMouseleaveListener&&(this.bindContainerMouseleaveListener(),this.containerMouseleaveListener=null)}show(){if(!this.getOption(`tooltipLabel`)||this.getOption(`disabled`))return;this.create();let e=this.container;this.el.nativeElement.closest(`p-dialog`)?setTimeout(()=>{this.container&&(this.container.style.display=`inline-block`,this.align())},100):(e.style.display=`inline-block`,this.align()),ed(e,250),this.getOption(`tooltipZIndex`)===`auto`?Kt.set(`tooltip`,e,this.config.zIndex.tooltip):e.style.zIndex=this.getOption(`tooltipZIndex`),this.bindDocumentResizeListener(),this.bindScrollListener()}hide(){this.getOption(`tooltipZIndex`)===`auto`&&Kt.clear(this.container),this.remove()}updateText(){if(!this.tooltipText)return;let e=this.getOption(`tooltipLabel`);if(e&&typeof e.createEmbeddedView==`function`){let t=this.viewContainer.createEmbeddedView(e);t.detectChanges(),t.rootNodes.forEach(o=>this.tooltipText.appendChild(o))}else this.getOption(`escape`)?(this.tooltipText.innerHTML=``,this.tooltipText.appendChild(document.createTextNode(e))):this.tooltipText.innerHTML=e}align(){let e=this.getOption(`tooltipPosition`),o={top:[this.alignTop,this.alignBottom,this.alignRight,this.alignLeft],bottom:[this.alignBottom,this.alignTop,this.alignRight,this.alignLeft],left:[this.alignLeft,this.alignRight,this.alignTop,this.alignBottom],right:[this.alignRight,this.alignLeft,this.alignTop,this.alignBottom]}[e]||[];for(let[r,s]of o.entries())if(r===0)s.call(this);else if(this.isOutOfBounds())s.call(this);else break}getHostOffset(){if(this.getOption(`appendTo`)===`body`||this.getOption(`appendTo`)===`target`){let e=this.el.nativeElement.getBoundingClientRect();return{left:e.left+Kl(),top:e.top+Yl()}}else return{left:0,top:0}}get activeElement(){return this.el.nativeElement.nodeName.startsWith(`P-`)?td(this.el.nativeElement,`.p-component`):this.el.nativeElement}alignRight(){this.preAlign(`right`);let e=this.activeElement,t=Zl(e),o=(od(e)-od(this.container))/2;this.alignTooltip(t,o);let r=this.getArrowElement();r&&(r.style.top=`50%`,r.style.right=``,r.style.bottom=``,r.style.left=`0`)}alignLeft(){this.preAlign(`left`);let e=this.getArrowElement(),t=Zl(this.container),o=(od(this.el.nativeElement)-od(this.container))/2;this.alignTooltip(-t,o),e&&(e.style.top=`50%`,e.style.right=`0`,e.style.bottom=``,e.style.left=``)}alignTop(){this.preAlign(`top`);let e=this.getArrowElement(),t=this.getHostOffset(),o=Zl(this.container),r=(Zl(this.el.nativeElement)-Zl(this.container))/2,s=od(this.container);this.alignTooltip(r,-s);let a=t.left-this.getHostOffset().left+o/2;e&&(e.style.top=``,e.style.right=``,e.style.bottom=`0`,e.style.left=a+`px`)}getArrowElement(){return td(this.container,`[data-pc-section="arrow"]`)}alignBottom(){this.preAlign(`bottom`);let e=this.getArrowElement(),t=Zl(this.container),o=this.getHostOffset(),r=(Zl(this.el.nativeElement)-Zl(this.container))/2,s=od(this.el.nativeElement);this.alignTooltip(r,s);let a=o.left-this.getHostOffset().left+t/2;e&&(e.style.top=`0`,e.style.right=``,e.style.bottom=``,e.style.left=a+`px`)}alignTooltip(e,t){let o=this.getHostOffset(),r=o.left+e,s=o.top+t;this.container.style.left=r+this.getOption(`positionLeft`)+`px`,this.container.style.top=s+this.getOption(`positionTop`)+`px`}getOption(e){return this._tooltipOptions()[e]}getTarget(e){return Ko$1(e,`p-inputwrapper`)?td(e,`input`):e}preAlign(e){this.container.style.left=`-999px`,this.container.style.top=`-999px`,this.container.className=this.cn(this.cx(`root`),this.ptm(`root`)?.class,`p-tooltip-`+e,this.getOption(`tooltipStyleClass`)??``)??``}isOutOfBounds(){let e=this.container.getBoundingClientRect(),t=e.top,o=e.left,r=Zl(this.container),s=od(this.container),a=Gl$1();return o+r>a.width||o<0||t<0||t+s>a.height}onWindowResize(e){this.hide()}bindDocumentResizeListener(){let e=this.onWindowResize.bind(this);this.resizeListener=e,window.addEventListener(`resize`,e)}unbindDocumentResizeListener(){this.resizeListener&&(window.removeEventListener(`resize`,this.resizeListener),this.resizeListener=null)}bindScrollListener(){this.scrollHandler||(this.scrollHandler=new Vt(this.el.nativeElement,()=>{this.container&&this.hide()})),this.scrollHandler.bindScrollListener()}unbindScrollListener(){this.scrollHandler&&this.scrollHandler.unbindScrollListener()}unbindEvents(){let e=this.getOption(`tooltipEvent`);if((e===`hover`||e===`both`)&&(this.el.nativeElement.removeEventListener(`mouseenter`,this.mouseEnterListener),this.el.nativeElement.removeEventListener(`mouseleave`,this.mouseLeaveListener),this.el.nativeElement.removeEventListener(`click`,this.clickListener),this.el.nativeElement.removeEventListener(`touchstart`,this.touchStartListener),this.el.nativeElement.removeEventListener(`touchend`,this.touchEndListener),this.unbindDocumentTouchListener()),e===`focus`||e===`both`){let t=this.el.nativeElement.querySelector(`.p-component`);t||(t=this.getTarget(this.el.nativeElement)),t.removeEventListener(`focus`,this.focusListener),t.removeEventListener(`blur`,this.blurListener)}this.unbindDocumentResizeListener()}remove(){this.container&&this.container.parentElement&&(this.getOption(`appendTo`)===`body`?document.body.removeChild(this.container):this.getOption(`appendTo`)===`target`?this.el.nativeElement.removeChild(this.container):cd(this.getOption(`appendTo`),this.container)),this.unbindDocumentResizeListener(),this.unbindScrollListener(),this.unbindContainerMouseleaveListener(),this.unbindDocumentTouchListener(),this.clearTimeouts(),this.container=null,this.scrollHandler=null}clearShowTimeout(){this.showTimeout&&(clearTimeout(this.showTimeout),this.showTimeout=null)}clearHideTimeout(){this.hideTimeout&&(clearTimeout(this.hideTimeout),this.hideTimeout=null)}clearTimeouts(){this.clearShowTimeout(),this.clearHideTimeout()}onDestroy(){this.unbindEvents(),this.container&&Kt.clear(this.container),this.remove(),this.scrollHandler&&(this.scrollHandler.destroy(),this.scrollHandler=null),this.documentEscapeListener&&this.documentEscapeListener()}static ɵfac=function(t){return new(t||n)};static ɵdir=CI({type:n,selectors:[[``,`pTooltip`,``]],inputs:{tooltipPosition:[1,`tooltipPosition`],tooltipEvent:[1,`tooltipEvent`],positionStyle:[1,`positionStyle`],tooltipStyleClass:[1,`tooltipStyleClass`],tooltipZIndex:[1,`tooltipZIndex`],escape:[1,`escape`],showDelay:[1,`showDelay`],hideDelay:[1,`hideDelay`],life:[1,`life`],positionTop:[1,`positionTop`],positionLeft:[1,`positionLeft`],autoHide:[1,`autoHide`],fitContent:[1,`fitContent`],hideOnEscape:[1,`hideOnEscape`],showOnEllipsis:[1,`showOnEllipsis`],content:[1,`pTooltip`,`content`],tooltipDisabled:[1,`tooltipDisabled`],tooltipOptions:[1,`tooltipOptions`],appendTo:[1,`appendTo`],pTooltipPT:[1,`pTooltipPT`],pTooltipUnstyled:[1,`pTooltipUnstyled`]},features:[QE([Co,{provide:To,useExisting:n},{provide:X,useExisting:n}]),Yf]})}return n})();var Eo=(()=>{class n{static ɵfac=function(t){return new(t||n)};static ɵmod=DI({type:n});static ɵinj=Il({imports:[ve,ve]})}return n})();var qt=class n{constructor(i){this.sanitizer=i}sanitizer;transform(i){let e=Fs(i).replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,t=>/^"/.test(t)?/:$/.test(t)?`<span class="text-slate-700 dark:text-slate-300">${t}</span>`:`<span class="text-emerald-600 dark:text-emerald-400">${t}</span>`:/true|false/.test(t)?`<span class="text-amber-600 dark:text-amber-400">${t}</span>`:/null/.test(t)?`<span class="text-slate-400 dark:text-slate-500 italic">${t}</span>`:`<span class="text-emerald-600 dark:text-emerald-400">${t}</span>`);return this.sanitizer.bypassSecurityTrustHtml(e)}static ɵfac=function(e){return new(e||n)(mr$1(Ho$1,16))};static ɵpipe=bI({name:`jsonHighlight`,type:n,pure:!0})};function Fs(n){return n.replace(/&/g,`&amp;`).replace(/</g,`&lt;`).replace(/>/g,`&gt;`)}var So=`
    .p-skeleton {
        display: block;
        overflow: hidden;
        background: dt('skeleton.background');
        border-radius: dt('skeleton.border.radius');
    }

    .p-skeleton::after {
        content: '';
        animation: p-skeleton-animation 1.2s infinite;
        height: 100%;
        left: 0;
        position: absolute;
        right: 0;
        top: 0;
        transform: translateX(-100%);
        z-index: 1;
        background: linear-gradient(90deg, rgba(255, 255, 255, 0), dt('skeleton.animation.background'), rgba(255, 255, 255, 0));
    }

    [dir='rtl'] .p-skeleton::after {
        animation-name: p-skeleton-animation-rtl;
    }

    .p-skeleton-circle {
        border-radius: 50%;
    }

    .p-skeleton-animation-none::after {
        animation: none;
    }

    @keyframes p-skeleton-animation {
        from {
            transform: translateX(-100%);
        }
        to {
            transform: translateX(100%);
        }
    }

    @keyframes p-skeleton-animation-rtl {
        from {
            transform: translateX(100%);
        }
        to {
            transform: translateX(-100%);
        }
    }
`;var Bs={root:{position:`relative`}};var Ps={root:({instance:n})=>[`p-skeleton p-component`,{"p-skeleton-circle":n.shape()===`circle`,"p-skeleton-animation-none":n.animation()===`none`}]};var ko=(()=>{class n extends $i$1{name=`skeleton`;style=So;classes=Ps;inlineStyles=Bs;static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵprov=pe({token:n,factory:n.ɵfac})}return n})();var No=new _(`SKELETON_INSTANCE`);var hn=(()=>{class n extends G{componentName=`Skeleton`;$pcSkeleton=C(No,{optional:!0,skipSelf:!0})??void 0;bindDirectiveInstance=C($,{self:!0});shape=mL(`rectangle`);animation=mL(`wave`);borderRadius=mL();size=mL();width=mL(`100%`);height=mL(`1rem`);_componentStyle=C(ko);containerStyle=sD(()=>{let e=this._componentStyle?.inlineStyles.root,t=this.size(),o=this.width(),r=this.height(),s=this.borderRadius();if(!this.$unstyled())return t?W(G$1({},e),{width:t,height:t,borderRadius:s}):W(G$1({},e),{width:o,height:r,borderRadius:s})});dataP=sD(()=>{let e=this.shape();return this.cn({[e]:e})});onAfterViewChecked(){this.bindDirectiveInstance.setAttrs(this.ptms([`host`,`root`]))}static ɵfac=(()=>{let e;return function(o){return(e||(e=mm(n)))(o||n)}})();static ɵcmp=II({type:n,selectors:[[`p-skeleton`]],hostVars:6,hostBindings:function(t,o){t&2&&(np(`aria-hidden`,!0)(`data-p`,o.dataP()),NE(o.containerStyle()),xE(o.cx(`root`)))},inputs:{shape:[1,`shape`],animation:[1,`animation`],borderRadius:[1,`borderRadius`],size:[1,`size`],width:[1,`width`],height:[1,`height`]},features:[QE([ko,{provide:No,useExisting:n},{provide:X,useExisting:n}]),NI([$]),Yf],decls:0,vars:0,template:function(t,o){},dependencies:[Dd],encapsulation:2})}return n})();var Mo=(()=>{class n{static ɵfac=function(t){return new(t||n)};static ɵmod=DI({type:n});static ɵinj=Il({imports:[hn,Dd,Dd]})}return n})();var Gt=class n{static ɵfac=function(e){return new(e||n)};static ɵcmp=II({type:n,selectors:[[`app-detail-skeleton`]],hostAttrs:[1,`block`],decls:6,vars:0,consts:[[1,`flex`,`flex-col`,`gap-4`,`pt-1`],[`width`,`40%`,`height`,`1rem`],[`width`,`25%`,`height`,`0.75rem`],[`width`,`60%`,`height`,`0.875rem`,`styleClass`,`mt-4`],[`width`,`50%`,`height`,`0.875rem`],[`height`,`10rem`,`styleClass`,`mt-4`]],template:function(e,t){e&1&&(ni(0,`div`,0),op(1,`p-skeleton`,1)(2,`p-skeleton`,2)(3,`p-skeleton`,3)(4,`p-skeleton`,4)(5,`p-skeleton`,5),gc())},dependencies:[Mo,hn],encapsulation:2})};function Io(n,i){setTimeout(i,Math.max(0,300-(Date.now()-n)))}async function Qt(n){if(navigator.clipboard&&window.isSecureContext)try{return await navigator.clipboard.writeText(n),!0}catch{}let i=document.activeElement,e=document.createElement(`textarea`);e.value=n,e.setAttribute(`readonly`,``),e.style.cssText=`position:fixed;top:0;left:0;opacity:0;pointer-events:none`,(i?.parentElement??document.body).appendChild(e),e.select();try{return document.execCommand(`copy`)}catch{return!1}finally{e.remove(),i?.focus()}}var js=[400,422,503];function Lo(n,i){return n instanceof ze&&js.includes(n.status)&&typeof n.error==`string`&&n.error.trim()?n.error:i}function Zt(n){let i=G$1({},n);return delete i[`@class`],i}function mn(n){return JSON.stringify(Zt(n),null,2)}function $o(n){return Object.entries(n??{}).map(([i,e])=>({key:i,value:e}))}function Hs(n){var i;let e=/^\/(.*)\/([gimyu]*)$/.exec(n.toString());if(!e)throw new Error(`Invalid RegExp`);return new RegExp((i=e[1])!==null&&i!==void 0?i:``,e[2])}function He(n){if(typeof n!=`object`)return n;if(n===null)return null;if(Array.isArray(n))return n.map(He);if(n instanceof Date)return new Date(n.getTime());if(n instanceof RegExp)return Hs(n);let i={};for(let e in n)Object.prototype.hasOwnProperty.call(n,e)&&(i[e]=He(n[e]));return i}function Ao(n,i){if(n.length===0)throw new Error(i||`Expected a non-empty array`)}function Oo(n,i){if(n.length<2)throw new Error(i||`Expected an array with at least 2 items`)}var Fo=n=>n[n.length-1];var Ae=class{setResult(i){return this.result=i,this.hasResult=!0,this}exit(){return this.exiting=!0,this}push(i,e){return i.parent=this,typeof e<`u`&&(i.childName=e),i.root=this.root||this,i.options=i.options||this.options,this.children?(Ao(this.children),Fo(this.children).next=i,this.children.push(i)):(this.children=[i],this.nextAfterChildren=this.next||null,this.next=i),i.next=this,this}};var gn=class extends Ae{constructor(i,e){super(),this.left=i,this.right=e,this.pipe=`diff`}prepareDeltaResult(i){var e,t,o,r;if(typeof i==`object`&&(!((e=this.options)===null||e===void 0)&&e.omitRemovedValues&&Array.isArray(i)&&i.length>1&&(i.length===2||i[2]===0||i[2]===3)&&(i[0]=0),!((t=this.options)===null||t===void 0)&&t.cloneDiffValues)){let s=typeof((o=this.options)===null||o===void 0?void 0:o.cloneDiffValues)==`function`?(r=this.options)===null||r===void 0?void 0:r.cloneDiffValues:He;typeof i[0]==`object`&&(i[0]=s(i[0])),typeof i[1]==`object`&&(i[1]=s(i[1]))}return i}setResult(i){return this.prepareDeltaResult(i),super.setResult(i)}};var Se=gn;var bn=class extends Ae{constructor(i,e){super(),this.left=i,this.delta=e,this.pipe=`patch`}};var et=bn;var vn=class extends Ae{constructor(i){super(),this.delta=i,this.pipe=`reverse`}};var tt=vn;var yn=class{constructor(i){this.name=i,this.filters=[]}process(i){if(!this.processor)throw new Error(`add this pipe to a processor before using it`);let e=this.debug,t=this.filters.length,o=i;for(let r=0;r<t;r++){let s=this.filters[r];if(s&&(e&&this.log(`filter: ${s.filterName}`),s(o),typeof o==`object`&&o.exiting)){o.exiting=!1;break}}!o.next&&this.resultCheck&&this.resultCheck(o)}log(i){console.log(`[jsondiffpatch] ${this.name} pipe, ${i}`)}append(...i){return this.filters.push(...i),this}prepend(...i){return this.filters.unshift(...i),this}indexOf(i){if(!i)throw new Error(`a filter name is required`);for(let e=0;e<this.filters.length;e++)if(this.filters[e]?.filterName===i)return e;throw new Error(`filter not found: ${i}`)}list(){return this.filters.map(i=>i.filterName)}after(i,...e){let t=this.indexOf(i);return this.filters.splice(t+1,0,...e),this}before(i,...e){let t=this.indexOf(i);return this.filters.splice(t,0,...e),this}replace(i,...e){let t=this.indexOf(i);return this.filters.splice(t,1,...e),this}remove(i){let e=this.indexOf(i);return this.filters.splice(e,1),this}clear(){return this.filters.length=0,this}shouldHaveResult(i){return i===!1?(this.resultCheck=null,this):this.resultCheck?this:(this.resultCheck=e=>{if(!e.hasResult){console.log(e);let t=new Error(`${this.name} failed`);throw t.noResult=!0,t}},this)}};var Xt=yn;var xn=class{constructor(i){this.selfOptions=i||{},this.pipes={}}options(i){return i&&(this.selfOptions=i),this.selfOptions}pipe(i,e){let t=e;if(typeof i==`string`){if(typeof t>`u`)return this.pipes[i];this.pipes[i]=t}if(i&&i.name){if(t=i,t.processor===this)return t;this.pipes[t.name]=t}if(!t)throw new Error(`pipe is not defined: ${i}`);return t.processor=this,t}process(i,e){let t=i;t.options=this.options();let o=e||i.pipe||`default`,r;for(;o;)typeof t.nextAfterChildren<`u`&&(t.next=t.nextAfterChildren,t.nextAfterChildren=null),typeof o==`string`&&(o=this.pipe(o)),o.process(t),r=o,o=null,t&&t.next&&(t=t.next,o=t.pipe||r);return t.hasResult?t.result:void 0}};var Bo=xn;var Vs=(n,i,e,t)=>n[e]===i[t];var zs=(n,i,e,t)=>{var o,r,s;let a=n.length,d=i.length,c,l,b=new Array(a+1);for(c=0;c<a+1;c++){let x=new Array(d+1);for(l=0;l<d+1;l++)x[l]=0;b[c]=x}for(b.match=e,c=1;c<a+1;c++){let x=b[c];if(x===void 0)throw new Error(`LCS matrix row is undefined`);let h=b[c-1];if(h===void 0)throw new Error(`LCS matrix row is undefined`);for(l=1;l<d+1;l++)e(n,i,c-1,l-1,t)?x[l]=((o=h[l-1])!==null&&o!==void 0?o:0)+1:x[l]=Math.max((r=h[l])!==null&&r!==void 0?r:0,(s=x[l-1])!==null&&s!==void 0?s:0)}return b};var Ws=(n,i,e,t)=>{let o=i.length,r=e.length,s={sequence:[],indices1:[],indices2:[]};for(;o!==0&&r!==0;){if(n.match===void 0)throw new Error(`LCS matrix match function is undefined`);if(n.match(i,e,o-1,r-1,t))s.sequence.unshift(i[o-1]),s.indices1.unshift(o-1),s.indices2.unshift(r-1),--o,--r;else{let d=n[o];if(d===void 0)throw new Error(`LCS matrix row is undefined`);let c=d[r-1];if(c===void 0)throw new Error(`LCS matrix value is undefined`);let l=n[o-1];if(l===void 0)throw new Error(`LCS matrix row is undefined`);let b=l[r];if(b===void 0)throw new Error(`LCS matrix value is undefined`);c>b?--r:--o}}return s};var Us=(n,i,e,t)=>{let o=t||{};return Ws(zs(n,i,e||Vs,o),n,i,o)};var Po={get:Us};var Ve=3;function Ks(n,i,e,t){for(let o=0;o<e;o++){let r=n[o];for(let s=0;s<t;s++){let a=i[s];if(o!==s&&r===a)return!0}}return!1}function Jt(n,i,e,t,o){let r=n[e],s=i[t];if(r===s)return!0;if(typeof r!=`object`||typeof s!=`object`)return!1;let a=o.objectHash;if(!a)return o.matchByPosition&&e===t;o.hashCache1=o.hashCache1||[];let d=o.hashCache1[e];if(typeof d>`u`&&(o.hashCache1[e]=d=a(r,e)),typeof d>`u`)return!1;o.hashCache2=o.hashCache2||[];let c=o.hashCache2[t];return typeof c>`u`&&(o.hashCache2[t]=c=a(s,t)),typeof c>`u`?!1:d===c}var _n=function(i){var e,t,o,r,s;if(!i.leftIsArray)return;let a={objectHash:(e=i.options)===null||e===void 0?void 0:e.objectHash,matchByPosition:(t=i.options)===null||t===void 0?void 0:t.matchByPosition},d=0,c=0,l,b,x,h=i.left,M=i.right,j=h.length,_=M.length,I;for(j>0&&_>0&&!a.objectHash&&typeof a.matchByPosition!=`boolean`&&(a.matchByPosition=!Ks(h,M,j,_));d<j&&d<_&&Jt(h,M,d,d,a);)l=d,I=new Se(h[l],M[l]),i.push(I,l),d++;for(;c+d<j&&c+d<_&&Jt(h,M,j-1-c,_-1-c,a);)b=j-1-c,x=_-1-c,I=new Se(h[b],M[x]),i.push(I,x),c++;let F;if(d+c===j){if(j===_){i.setResult(void 0).exit();return}for(F=F||{_t:`a`},l=d;l<_-c;l++)F[l]=[M[l]],i.prepareDeltaResult(F[l]);i.setResult(F).exit();return}if(d+c===_){for(F=F||{_t:`a`},l=d;l<j-c;l++){let de=`_${l}`;F[de]=[h[l],0,0],i.prepareDeltaResult(F[de])}i.setResult(F).exit();return}a.hashCache1=void 0,a.hashCache2=void 0;let S=h.slice(d,j-c),ae=M.slice(d,_-c),le=Po.get(S,ae,Jt,a),ke=[];for(F=F||{_t:`a`},l=d;l<j-c;l++)if(le.indices1.indexOf(l-d)<0){let de=`_${l}`;F[de]=[h[l],0,0],i.prepareDeltaResult(F[de]),ke.push(l)}let xt=!0;!((o=i.options)===null||o===void 0)&&o.arrays&&i.options.arrays.detectMove===!1&&(xt=!1);let ze=!1;!((s=(r=i.options)===null||r===void 0?void 0:r.arrays)===null||s===void 0)&&s.includeValueOnMove&&(ze=!0);let it=ke.length;for(l=d;l<_-c;l++){let de=le.indices2.indexOf(l-d);if(de<0){let Wn=!1;if(xt&&it>0)for(let _t=0;_t<it;_t++){b=ke[_t];let wt=b===void 0?void 0:F[`_${b}`];if(b!==void 0&&wt&&Jt(S,ae,b-d,l-d,a)){wt.splice(1,2,l,Ve),wt.splice(1,2,l,Ve),ze||(wt[0]=``),x=l,I=new Se(h[b],M[x]),i.push(I,x),ke.splice(_t,1),Wn=!0;break}}Wn||(F[l]=[M[l]],i.prepareDeltaResult(F[l]))}else{if(le.indices1[de]===void 0)throw new Error(`Invalid indexOnArray2: ${de}, seq.indices1: ${le.indices1}`);if(b=le.indices1[de]+d,le.indices2[de]===void 0)throw new Error(`Invalid indexOnArray2: ${de}, seq.indices2: ${le.indices2}`);x=le.indices2[de]+d,I=new Se(h[b],M[x]),i.push(I,x)}}i.setResult(F).exit()};_n.filterName=`arrays`;var Ro={numerically(n,i){return n-i},numericallyBy(n){return(i,e)=>i[n]-e[n]}};var wn=function(i){var e;if(!i.nested)return;let t=i.delta;if(t._t!==`a`)return;let o,r,s=t,a=i.left,d=[],c=[],l=[];for(o in s)if(o!==`_t`)if(o[0]===`_`){let h=o;if(s[h]!==void 0&&(s[h][2]===0||s[h][2]===Ve))d.push(Number.parseInt(o.slice(1),10));else throw new Error(`only removal or move can be applied at original array indices, invalid diff type: ${(e=s[h])===null||e===void 0?void 0:e[2]}`)}else{let h=o;s[h].length===1?c.push({index:Number.parseInt(h,10),value:s[h][0]}):l.push({index:Number.parseInt(h,10),delta:s[h]})}for(d=d.sort(Ro.numerically),o=d.length-1;o>=0;o--){if(r=d[o],r===void 0)continue;let h=s[`_${r}`],M=a.splice(r,1)[0];h?.[2]===Ve&&c.push({index:h[1],value:M})}c=c.sort(Ro.numericallyBy(`index`));let b=c.length;for(o=0;o<b;o++){let h=c[o];h!==void 0&&a.splice(h.index,0,h.value)}let x=l.length;if(x>0)for(o=0;o<x;o++){let h=l[o];if(h===void 0)continue;let M=new et(a[h.index],h.delta);i.push(M,h.index)}if(!i.children){i.setResult(a).exit();return}i.exit()};wn.filterName=`arrays`;var Cn=function(i){if(!i||!i.children||i.delta._t!==`a`)return;let t=i.left,o=i.children.length;for(let r=0;r<o;r++){let s=i.children[r];if(s===void 0)continue;let a=s.childName;t[a]=s.result}i.setResult(t).exit()};Cn.filterName=`arraysCollectChildren`;var Tn=function(i){if(!i.nested){let o=i.delta;if(o[2]===Ve){let r=o;i.newName=`_${r[1]}`,i.setResult([r[0],Number.parseInt(i.childName.substring(1),10),Ve]).exit()}return}let e=i.delta;if(e._t!==`a`)return;let t=e;for(let o in t){if(o===`_t`)continue;let r=new tt(t[o]);i.push(r,o)}i.exit()};Tn.filterName=`arrays`;var qs=(n,i,e)=>{if(typeof i==`string`&&i[0]===`_`)return Number.parseInt(i.substring(1),10);if(Array.isArray(e)&&e[2]===0)return`_${i}`;let t=+i;for(let o in n){let r=n[o];if(Array.isArray(r))if(r[2]===Ve){let s=Number.parseInt(o.substring(1),10),a=r[1];if(a===+i)return s;s<=t&&a>t?t++:s>=t&&a<t&&t--}else r[2]===0?Number.parseInt(o.substring(1),10)<=t&&t++:r.length===1&&Number.parseInt(o,10)<=t&&t--}return t};var Dn=n=>{if(!n||!n.children)return;let i=n.delta;if(i._t!==`a`)return;let e=i,t=n.children.length,o={_t:`a`};for(let r=0;r<t;r++){let s=n.children[r];if(s===void 0)continue;let a=s.newName;if(typeof a>`u`){if(s.childName===void 0)throw new Error(`child.childName is undefined`);a=qs(e,s.childName,s.result)}o[a]!==s.result&&(o[a]=s.result)}n.setResult(o).exit()};Dn.filterName=`arraysCollectChildren`;var En=function(i){i.left instanceof Date?(i.right instanceof Date?i.left.getTime()!==i.right.getTime()?i.setResult([i.left,i.right]):i.setResult(void 0):i.setResult([i.left,i.right]),i.exit()):i.right instanceof Date&&i.setResult([i.left,i.right]).exit()};En.filterName=`dates`;var Yt=new Set([`__proto__`]);var Sn=n=>{if(!n||!n.children)return;let i=n.children.length,e=n.result;for(let t=0;t<i;t++){let o=n.children[t];if(o!==void 0&&!(typeof o.result>`u`)){if(e=e||{},o.childName===void 0)throw new Error(`diff child.childName is undefined`);e[o.childName]=o.result}}e&&n.leftIsArray&&(e._t=`a`),n.setResult(e).exit()};Sn.filterName=`collectChildren`;var kn=n=>{var i;if(n.leftIsArray||n.leftType!==`object`)return;let e=n.left,t=n.right,o=(i=n.options)===null||i===void 0?void 0:i.propertyFilter;for(let r in e){if(!Object.prototype.hasOwnProperty.call(e,r)||o&&!o(r,n))continue;let s=new Se(e[r],t[r]);n.push(s,r)}for(let r in t)if(Object.prototype.hasOwnProperty.call(t,r)&&!(o&&!o(r,n))&&typeof e[r]>`u`){let s=new Se(void 0,t[r]);n.push(s,r)}if(!n.children||n.children.length===0){n.setResult(void 0).exit();return}n.exit()};kn.filterName=`objects`;var Nn=function(i){if(!i.nested)return;let e=i.delta;if(e._t)return;let t=e,o=!1;for(let r in t){if(Yt.has(r)||!Object.prototype.hasOwnProperty.call(t,r))continue;let s=i.left,d=new et(s!==null&&typeof s==`object`&&Object.prototype.hasOwnProperty.call(s,r)?s[r]:void 0,t[r]);i.push(d,r),o=!0}if(!o){i.setResult(i.left).exit();return}i.exit()};Nn.filterName=`objects`;var Mn=function(i){if(!i||!i.children||i.delta._t)return;if(i.left===null||typeof i.left!=`object`){i.setResult(i.left).exit();return}let t=i.left,o=i.children.length;for(let r=0;r<o;r++){let s=i.children[r];if(s===void 0)continue;let a=s.childName;Yt.has(a)||(Object.prototype.hasOwnProperty.call(i.left,a)&&s.result===void 0?delete t[a]:t[a]!==s.result&&(t[a]=s.result))}i.setResult(t).exit()};Mn.filterName=`collectChildren`;var In=function(i){if(!i.nested||i.delta._t)return;let t=i.delta,o=!1;for(let r in t){if(Yt.has(r)||!Object.prototype.hasOwnProperty.call(t,r))continue;let s=new tt(t[r]);i.push(s,r),o=!0}if(!o){i.setResult({}).exit();return}i.exit()};In.filterName=`objects`;var Ln=n=>{if(!n||!n.children||n.delta._t)return;let e=n.children.length,t={};for(let o=0;o<e;o++){let r=n.children[o];if(r===void 0)continue;let s=r.childName;Yt.has(s)||t[s]!==r.result&&(t[s]=r.result)}n.setResult(t).exit()};Ln.filterName=`collectChildren`;var $n=null;function jo(n,i){var e;if(!$n){let t;if(!((e=n?.textDiff)===null||e===void 0)&&e.diffMatchPatch)t=new n.textDiff.diffMatchPatch;else{if(!i)return null;let o=new Error("The diff-match-patch library was not provided. Pass the library in through the options or use the `jsondiffpatch/with-text-diffs` entry-point.");throw o.diff_match_patch_not_found=!0,o}$n={diff:(o,r)=>t.patch_toText(t.patch_make(o,r)),patch:(o,r)=>{let s=t.patch_apply(t.patch_fromText(r),o);for(let a of s[1])if(!a){let d=new Error(`text patch failed`);throw d.textPatchFailed=!0,d}return s[0]}}}return $n}var An=function(i){var e,t;if(i.leftType!==`string`)return;let o=i.left,r=i.right,s=((t=(e=i.options)===null||e===void 0?void 0:e.textDiff)===null||t===void 0?void 0:t.minLength)||60;if(o.length<s||r.length<s){i.setResult([o,r]).exit();return}let a=jo(i.options);if(!a){i.setResult([o,r]).exit();return}let d=a.diff;i.setResult([d(o,r),0,2]).exit()};An.filterName=`texts`;var On=function(i){if(i.nested)return;let e=i.delta;if(e[2]!==2)return;let t=e,o=jo(i.options,!0).patch;i.setResult(o(i.left,t[0])).exit()};On.filterName=`texts`;var Xs=n=>{var i,e,t;let o=/^@@ +-(\d+),(\d+) +\+(\d+),(\d+) +@@$/,r=n.split(`
`);for(let s=0;s<r.length;s++){let a=r[s];if(a===void 0)continue;let d=a.slice(0,1);if(d===`@`){let c=o.exec(a);if(c!==null){let l=s;r[l]=`@@ -${c[3]},${c[4]} +${c[1]},${c[2]} @@`}}else if(d===`+`){if(r[s]=`-${(i=r[s])===null||i===void 0?void 0:i.slice(1)}`,((e=r[s-1])===null||e===void 0?void 0:e.slice(0,1))===`+`){let c=r[s];r[s]=r[s-1],r[s-1]=c}}else d===`-`&&(r[s]=`+${(t=r[s])===null||t===void 0?void 0:t.slice(1)}`)}return r.join(`
`)};var Fn=function(i){if(i.nested)return;let e=i.delta;if(e[2]!==2)return;let t=e;i.setResult([Xs(t[0]),0,2]).exit()};Fn.filterName=`texts`;var Bn=function(i){if(i.left===i.right){i.setResult(void 0).exit();return}if(typeof i.left>`u`){if(typeof i.right==`function`)throw new Error(`functions are not supported`);i.setResult([i.right]).exit();return}if(typeof i.right>`u`){i.setResult([i.left,0,0]).exit();return}if(typeof i.left==`function`||typeof i.right==`function`)throw new Error(`functions are not supported`);if(i.leftType=i.left===null?`null`:typeof i.left,i.rightType=i.right===null?`null`:typeof i.right,i.leftType!==i.rightType){i.setResult([i.left,i.right]).exit();return}if(i.leftType===`boolean`||i.leftType===`number`){i.setResult([i.left,i.right]).exit();return}if(i.leftType===`object`&&(i.leftIsArray=Array.isArray(i.left)),i.rightType===`object`&&(i.rightIsArray=Array.isArray(i.right)),i.leftIsArray!==i.rightIsArray){i.setResult([i.left,i.right]).exit();return}i.left instanceof RegExp&&(i.right instanceof RegExp?i.setResult([i.left.toString(),i.right.toString()]).exit():i.setResult([i.left,i.right]).exit())};Bn.filterName=`trivial`;var Pn=function(i){if(typeof i.delta>`u`){i.setResult(i.left).exit();return}if(i.nested=!Array.isArray(i.delta),i.nested)return;let e=i.delta;if(e.length===1){i.setResult(e[0]).exit();return}if(e.length===2){if(i.left instanceof RegExp){let t=/^\/(.*)\/([gimyu]+)$/.exec(e[1]);if(t?.[1]){i.setResult(new RegExp(t[1],t[2])).exit();return}}i.setResult(e[1]).exit();return}e.length===3&&e[2]===0&&i.setResult(void 0).exit()};Pn.filterName=`trivial`;var Rn=function(i){if(typeof i.delta>`u`){i.setResult(i.delta).exit();return}if(i.nested=!Array.isArray(i.delta),i.nested)return;let e=i.delta;if(e.length===1){i.setResult([e[0],0,0]).exit();return}if(e.length===2){i.setResult([e[1],e[0]]).exit();return}e.length===3&&e[2]===0&&i.setResult([e[0]]).exit()};Rn.filterName=`trivial`;var jn=class{constructor(i){this.processor=new Bo(i),this.processor.pipe(new Xt(`diff`).append(Sn,Bn,En,An,kn,_n).shouldHaveResult()),this.processor.pipe(new Xt(`patch`).append(Mn,Cn,Pn,On,Nn,wn).shouldHaveResult()),this.processor.pipe(new Xt(`reverse`).append(Ln,Dn,Rn,Fn,In,Tn).shouldHaveResult())}options(i){return this.processor.options(i)}diff(i,e){return this.processor.process(new Se(i,e))}patch(i,e){return this.processor.process(new et(i,e))}reverse(i){return this.processor.process(new tt(i))}unpatch(i,e){return this.patch(i,this.reverse(e))}clone(i){return He(i)}};var Ho=jn;function Vo(n){return new Ho(n)}var Hn=class{format(i,e){let t={};this.prepareContext(t);let o=t;return this.recurse(o,i,e),this.finalize(o)}prepareContext(i){i.buffer=[],i.out=function(...e){if(!this.buffer)throw new Error(`context buffer is not initialized`);this.buffer.push(...e)}}typeFormattterNotFound(i,e){throw new Error(`cannot format delta type: ${e}`)}typeFormattterErrorFormatter(i,e,t,o,r,s,a){}finalize({buffer:i}){return Array.isArray(i)?i.join(``):``}recurse(i,e,t,o,r,s,a){let c=e&&s?s.value:t;if(typeof e>`u`&&typeof o>`u`)return;let l=this.getDeltaType(e,s),b=l===`node`?e._t===`a`?`array`:`object`:``;typeof o<`u`?this.nodeBegin(i,o,r,l,b,a??!1):this.rootBegin(i,l,b);let x;try{x=l!==`unknown`?this[`format_${l}`]:this.typeFormattterNotFound(i,l),x.call(this,i,e,c,o,r,s)}catch(h){this.typeFormattterErrorFormatter(i,h,e,c,o,r,s),typeof console<`u`&&console.error&&console.error(h.stack)}typeof o<`u`?this.nodeEnd(i,o,r,l,b,a??!1):this.rootEnd(i,l,b)}formatDeltaChildren(i,e,t){this.forEachDeltaKey(e,t,(o,r,s,a)=>{this.recurse(i,e[o],t?t[r]:void 0,o,r,s,a)})}forEachDeltaKey(i,e,t){let o=[];if(!(i._t===`a`)){let _=Object.keys(i);typeof e==`object`&&e!==null&&o.push(...Object.keys(e));for(let I of _)o.indexOf(I)>=0||o.push(I);for(let I=0;I<o.length;I++){let F=o[I];if(F===void 0)continue;t(F,F,void 0,I===o.length-1)}return}let s={};for(let _ in i)if(Object.prototype.hasOwnProperty.call(i,_)){let I=i[_];if(Array.isArray(I)&&I[2]===3){let F=I;s[F[1]]=Number.parseInt(_.substring(1))}}let a=i,d=0,c=0,l=Array.isArray(e)?e:void 0,b=l?l.length:Object.keys(a).reduce((_,I)=>{if(I===`_t`)return _;if(I.substring(0,1)===`_`){let ke=a[I],xt=Number.parseInt(I.substring(1)),ze=Array.isArray(ke)&&ke.length>=3&&ke[2]===3?ke[1]:void 0,it=Math.max(xt,ze??0);return it>_?it:_}let S=Number.parseInt(I),ae=s[S],le=Math.max(ae??0,S??0);return le>_?le:_},0)+1,x=b,h,M=(..._)=>{h&&t(...h),h=_},j=()=>{h&&t(h[0],h[1],h[2],!0)};for(;d<b||c<x||`${c}`in a;){let _=!1,I=`_${d}`,F=`${c}`,S=c in s?s[c]:void 0;if(I in a){_=!0;let ae=a[I];M(I,S??d,S?{key:`_${S}`,value:l?l[S]:void 0}:void 0,!1),Array.isArray(ae)?ae[2]===0?(x--,d++):(ae[2],d++):d++}if(F in a){_=!0;let ae=a[F],le=Array.isArray(ae)&&ae.length===1;M(F,S??d,S?{key:`_${S}`,value:l?l[S]:void 0}:void 0,!1),le?(x++,c++):(S===void 0&&d++,c++)}_||((l&&S===void 0||this.includeMoveDestinations!==!1)&&M(F,S??d,S?{key:`_${S}`,value:l?l[S]:void 0}:void 0,!1),S!==void 0||d++,c++)}j()}getDeltaType(i,e){if(typeof i>`u`)return typeof e<`u`?`movedestination`:`unchanged`;if(Array.isArray(i)){if(i.length===1)return`added`;if(i.length===2)return`modified`;if(i.length===3&&i[2]===0)return`deleted`;if(i.length===3&&i[2]===2)return`textdiff`;if(i.length===3&&i[2]===3)return`moved`}else if(typeof i==`object`)return`node`;return`unknown`}parseTextDiff(i){var e;let t=[],o=i.split(`
@@ `);for(let r of o){let s={pieces:[]},a=(e=/^(?:@@ )?[-+]?(\d+),(\d+)/.exec(r))===null||e===void 0?void 0:e.slice(1);if(!a)throw new Error(`invalid text diff format`);Oo(a),s.location={line:a[0],chr:a[1]};let d=r.split(`
`).slice(1);for(let c=0,l=d.length;c<l;c++){let b=d[c];if(b===void 0||!b.length)continue;let x={type:`context`};b.substring(0,1)===`+`?x.type=`added`:b.substring(0,1)===`-`&&(x.type=`deleted`),x.text=b.slice(1),s.pieces.push(x)}t.push(s)}return t}};var zo=Hn;var zn=class extends zo{typeFormattterErrorFormatter(i,e){let t=typeof e==`object`&&e!==null&&`message`in e&&typeof e.message==`string`?e.message:String(e);i.out(`<pre class="jsondiffpatch-error">${bt(t)}</pre>`)}formatValue(i,e){let t=typeof e>`u`?`undefined`:bt(JSON.stringify(e,null,2));i.out(`<pre>${t}</pre>`)}formatTextDiffString(i,e){let t=this.parseTextDiff(e);i.out(`<ul class="jsondiffpatch-textdiff">`);for(let o=0,r=t.length;o<r;o++){let s=t[o];if(s===void 0)return;i.out(`<li><div class="jsondiffpatch-textdiff-location"><span class="jsondiffpatch-textdiff-line-number">${s.location.line}</span><span class="jsondiffpatch-textdiff-char">${s.location.chr}</span></div><div class="jsondiffpatch-textdiff-line">`);let a=s.pieces;for(let d=0,c=a.length;d<c;d++){let l=a[d];if(l===void 0)return;i.out(`<span class="jsondiffpatch-textdiff-${l.type}">${bt(decodeURI(l.text))}</span>`)}i.out(`</div></li>`)}i.out(`</ul>`)}rootBegin(i,e,t){let o=`jsondiffpatch-${e}${t?` jsondiffpatch-child-node-type-${t}`:``}`;i.out(`<div class="jsondiffpatch-delta ${o}">`)}rootEnd(i){i.out(`</div>${i.hasArrows?`<script type="text/javascript">setTimeout(${ta.toString()},10);<\/script>`:``}`)}nodeBegin(i,e,t,o,r){let s=`jsondiffpatch-${o}${r?` jsondiffpatch-child-node-type-${r}`:``}`,a=typeof t==`number`&&e.substring(0,1)===`_`?e.substring(1):e;i.out(`<li class="${s}" data-key="${bt(e)}"><div class="jsondiffpatch-property-name">${bt(a)}</div>`)}nodeEnd(i){i.out(`</li>`)}format_unchanged(i,e,t){typeof t>`u`||(i.out(`<div class="jsondiffpatch-value">`),this.formatValue(i,t),i.out(`</div>`))}format_movedestination(i,e,t){typeof t>`u`||(i.out(`<div class="jsondiffpatch-value">`),this.formatValue(i,t),i.out(`</div>`))}format_node(i,e,t){let o=e._t===`a`?`array`:`object`;i.out(`<ul class="jsondiffpatch-node jsondiffpatch-node-type-${o}">`),this.formatDeltaChildren(i,e,t),i.out(`</ul>`)}format_added(i,e){i.out(`<div class="jsondiffpatch-value">`),this.formatValue(i,e[0]),i.out(`</div>`)}format_modified(i,e){i.out(`<div class="jsondiffpatch-value jsondiffpatch-left-value">`),this.formatValue(i,e[0]),i.out(`</div><div class="jsondiffpatch-value jsondiffpatch-right-value">`),this.formatValue(i,e[1]),i.out(`</div>`)}format_deleted(i,e){i.out(`<div class="jsondiffpatch-value">`),this.formatValue(i,e[0]),i.out(`</div>`)}format_moved(i,e){i.out(`<div class="jsondiffpatch-value">`),this.formatValue(i,e[0]),i.out(`</div><div class="jsondiffpatch-moved-destination">${e[1]}</div>`),i.out(`<div class="jsondiffpatch-arrow" style="position: relative; left: -34px;">
          <svg width="30" height="60" style="position: absolute; display: none;">
          <defs>
              <marker id="markerArrow" markerWidth="8" markerHeight="8"
                 refx="2" refy="4" stroke="#88f"
                     orient="auto" markerUnits="userSpaceOnUse">
                  <path d="M1,1 L1,7 L7,4 L1,1" style="fill: #339;" />
              </marker>
          </defs>
          <path d="M30,0 Q-10,25 26,50"
            style="stroke: #88f; stroke-width: 2px; fill: none; stroke-opacity: 0.5; marker-end: url(#markerArrow);"
          ></path>
          </svg>
      </div>`),i.hasArrows=!0}format_textdiff(i,e){i.out(`<div class="jsondiffpatch-value">`),this.formatTextDiffString(i,e[0]),i.out(`</div>`)}};function bt(n){if(typeof n==`number`)return n;let i=String(n);for(let t of[[/&/g,`&amp;`],[/</g,`&lt;`],[/>/g,`&gt;`],[/'/g,`&apos;`],[/"/g,`&quot;`]])i=i.replace(t[0],t[1]);return i}var ta=function(i){let e=i||document,t=({textContent:s,innerText:a})=>s||a,o=(s,a,d)=>{let c=s.querySelectorAll(a);for(let l=0,b=c.length;l<b;l++)d(c[l])},r=({children:s},a)=>{for(let d=0,c=s.length;d<c;d++){let l=s[d];l&&a(l,d)}};o(e,`.jsondiffpatch-arrow`,({parentNode:s,children:a,style:d})=>{let c=s,l=a[0],b=l.children[1];l.style.display=`none`;let x=c.querySelector(`.jsondiffpatch-moved-destination`);if(!(x instanceof HTMLElement))return;let h=t(x),M=c.parentNode;if(!M)return;let j;if(r(M,_=>{_.getAttribute(`data-key`)===h&&(j=_)}),!!j)try{let _=j.offsetTop-c.offsetTop;l.setAttribute(`height`,`${Math.abs(_)+6}`),d.top=`${-8+(_>0?0:_)}px`;let I=_>0?`M30,0 Q-10,${Math.round(_/2)} 26,${_-4}`:`M30,${-_} Q-10,${Math.round(-_/2)} 26,4`;b.setAttribute(`d`,I),l.style.display=``}catch(_){console.debug(`[jsondiffpatch] error adjusting arrows: ${_}`)}})};var Vn;function Wo(n,i){return Vn||(Vn=new zn),Vn.format(n,i)}var na=Vo();var en=class n{constructor(i){this.sanitizer=i}sanitizer;transform(i,e){let t=na.diff(e,i);return t?this.sanitizer.bypassSecurityTrustHtml(Wo(t,e)??``):this.sanitizer.bypassSecurityTrustHtml(`<span class="text-surface-400 text-xs">No changes</span>`)}static ɵfac=function(e){return new(e||n)(mr$1(Ho$1,16))};static ɵpipe=bI({name:`jsonDiff`,type:n,pure:!0})};var ia=(n,i)=>i.key;function oa(n,i){n&1&&op(0,`app-detail-skeleton`)}function ra(n,i){if(n&1&&op(0,`p-tag`,4),n&2)rp(`value`,`rev `+aE().event.revision)}function sa(n,i){if(n&1&&op(0,`p-tag`,18),n&2)rp(`value`,`v`+aE().state.version)}function aa(n,i){if(n&1){let e=eE();ni(0,`div`,20),op(1,`pre`,21),JE(2,`jsonHighlight`),ni(3,`p-button`,22),up(`onClick`,function(){ql(e);let o=aE(2);return Gl(o.copy(o.payloadJson(),`payload`))}),gc()()}if(n&2){let e=aE(2);Zy(),rp(`innerHTML`,eD(2,5,e.payloadJson()),ay),Zy(2),rp(`text`,!0)(`rounded`,!0)(`icon`,e.copiedKey()===`payload`?`pi pi-check`:`pi pi-copy`)(`pTooltip`,e.copiedKey()===`payload`?`Copied!`:`Copy`)}}function la(n,i){if(n&1){let e=eE();ni(0,`tr`,29)(1,`td`,30),HE(2),gc(),ni(3,`td`,31)(4,`div`,32)(5,`span`,33),HE(6),gc(),ni(7,`p-button`,34),up(`onClick`,function(){let o=ql(e).$implicit;return Gl(aE(4).copy(o.value,o.key))}),gc()()()()}if(n&2){let e=i.$implicit,t=aE(4);Zy(2),Sp(e.key),Zy(4),Sp(e.value),Zy(),rp(`text`,!0)(`rounded`,!0)(`icon`,t.copiedKey()===e.key?`pi pi-check`:`pi pi-copy`)(`pTooltip`,t.copiedKey()===e.key?`Copied!`:`Copy`)}}function da(n,i){if(n&1&&(ni(0,`div`,23)(1,`table`,25)(2,`thead`)(3,`tr`,26)(4,`th`,27),HE(5,`Key`),gc(),ni(6,`th`,28),HE(7,`Value`),gc()()(),ni(8,`tbody`),zI(9,la,8,6,`tr`,29,ia),gc()()()),n&2){let e=aE(3);Zy(9),QI(e.metadata())}}function ca(n,i){n&1&&(ni(0,`div`,24),HE(1,`No metadata.`),gc())}function ua(n,i){if(n&1&&qI(0,da,11,0,`div`,23)(1,ca,2,0,`div`,24),n&2)GI(aE(2).metadata().length>0?0:1)}function pa(n,i){n&1&&(ni(0,`pre`,36),HE(1,`// State unknown: the events before this one were deleted when a snapshot was taken`),gc())}function fa(n,i){if(n&1&&(op(0,`pre`,21),JE(1,`jsonDiff`)),n&2){let e=aE(3);rp(`innerHTML`,tD(1,1,e.diffCurrent(),e.diffPrevious()),ay)}}function ha(n,i){if(n&1&&(op(0,`pre`,21),JE(1,`jsonHighlight`)),n&2)rp(`innerHTML`,eD(1,1,aE(3).stateJson()),ay)}function ma(n,i){n&1&&(ni(0,`pre`,36),HE(1,`// No state`),gc())}function ga(n,i){if(n&1){let e=eE();ni(0,`p-button`,41),up(`onClick`,function(){ql(e);let o=aE(3);return Gl(o.showDiff.set(!o.showDiff()))}),gc()}if(n&2){let e=aE(3);rp(`text`,!0)(`rounded`,!0)(`severity`,e.showDiff()?`primary`:`secondary`)(`label`,e.showDiff()?`Hide diff`:`Show diff`)}}function ba(n,i){if(n&1){let e=eE();ni(0,`p-button`,10),up(`onClick`,function(){ql(e);let o=aE(3);return Gl(o.copy(o.stateJson(),`state`))}),gc()}if(n&2){let e=aE(3);rp(`text`,!0)(`rounded`,!0)(`icon`,e.copiedKey()===`state`?`pi pi-check`:`pi pi-copy`)(`pTooltip`,e.copiedKey()===`state`?`Copied!`:`Copy`)}}function va(n,i){n&1&&(ni(0,`p`,40),HE(1,`No diff: the state before this event is unknown, because the events before it were deleted when a snapshot was taken.`),gc())}function ya(n,i){if(n&1&&(ni(0,`div`,35),qI(1,pa,2,0,`pre`,36)(2,fa,2,4,`pre`,21)(3,ha,2,3,`pre`,21)(4,ma,2,0,`pre`,36),ni(5,`div`,37),qI(6,ga,1,4,`p-button`,38),qI(7,ba,1,4,`p-button`,39),gc()(),qI(8,va,2,0,`p`,40)),n&2){let e=aE(),t=aE();Zy(),GI(e.stateKnown?t.showDiff()&&t.canDiff()?2:e.state?3:4:1),Zy(5),GI(t.canDiff()?6:-1),Zy(),GI(e.state?7:-1),Zy(),GI(e.stateKnown&&!e.previousStateKnown?8:-1)}}function xa(n,i){if(n&1){let e=eE();ni(0,`div`,0)(1,`div`,1)(2,`span`,2),HE(3),gc(),ni(4,`span`,3),HE(5),gc(),qI(6,ra,1,1,`p-tag`,4),gc(),ni(7,`div`,5),HE(8),JE(9,`date`),gc()(),ni(10,`div`,6)(11,`div`,7)(12,`div`)(13,`div`,8),HE(14,`Event ID`),gc(),ni(15,`div`,9),HE(16),gc()(),ni(17,`p-button`,10),up(`onClick`,function(){let o=ql(e);return Gl(aE().copy(o.event.id,`event-id`))}),gc()(),ni(18,`div`,7)(19,`div`)(20,`div`,8),HE(21,`Aggregate ID`),gc(),ni(22,`div`,9),HE(23),gc()(),ni(24,`p-button`,10),up(`onClick`,function(){let o=ql(e);return Gl(aE().copy(o.event.aggregateId,`agg-id`))}),gc()()(),ni(25,`div`,11)(26,`p-tabs`,12),Op(`valueChange`,function(o){ql(e);let r=aE();return qE(r.activeTab,o)||(r.activeTab=o),Gl(o)}),ni(27,`p-tablist`,13)(28,`p-tab`,14),HE(29,`Payload`),gc(),ni(30,`p-tab`,15),HE(31,`Metadata`),gc(),ni(32,`p-tab`,16)(33,`span`,17),HE(34,`State `),qI(35,sa,1,1,`p-tag`,18),gc()()()()(),ni(36,`div`,19),qI(37,aa,4,7,`div`,20),qI(38,ua,2,1),qI(39,ya,9,4),gc()}if(n&2){let e=i,t=aE();Zy(3),Ec(`#`,e.event.sequence),Zy(2),Sp(e.event.type),Zy(),GI(e.event.revision>1?6:-1),Zy(2),Sp(tD(9,19,e.event.timestamp,`MMM d, y · HH:mm:ss.SSS`)),Zy(8),Sp(e.event.id),Zy(),rp(`text`,!0)(`rounded`,!0)(`icon`,t.copiedKey()===`event-id`?`pi pi-check`:`pi pi-copy`)(`pTooltip`,t.copiedKey()===`event-id`?`Copied!`:`Copy`),Zy(6),Sp(e.event.aggregateId),Zy(),rp(`text`,!0)(`rounded`,!0)(`icon`,t.copiedKey()===`agg-id`?`pi pi-check`:`pi pi-copy`)(`pTooltip`,t.copiedKey()===`agg-id`?`Copied!`:`Copy`),Zy(2),Rp(`value`,t.activeTab),Zy(9),GI(e.state?35:-1),Zy(2),GI(t.activeTab()===`payload`?37:-1),Zy(),GI(t.activeTab()===`metadata`?38:-1),Zy(),GI(t.activeTab()===`state`?39:-1)}}var nt=class n{svc=C(Ye);destroyRef=C(Ce);messageService=C(yd);event=mL(null);eventId=sD(()=>this.event()?.id);detail=_o$1(null);loading=_o$1(!1);loaded=gL();activeTab=_o$1(`payload`);showDiff=_o$1(!1);copiedKey=_o$1(null);payloadJson=sD(()=>{let i=this.detail();return i?mn(i.event.payload):``});metadata=sD(()=>$o(this.detail()?.event.metadata));stateJson=sD(()=>{let i=this.detail()?.state;return i?mn(i.payload):``});diffCurrent=sD(()=>Zt(this.detail()?.state?.payload??{}));diffPrevious=sD(()=>Zt(this.detail()?.previousState?.payload??{}));canDiff=sD(()=>{let i=this.detail();return!!i&&i.stateKnown&&i.previousStateKnown&&!!(i.state||i.previousState)});request;constructor(){hu(()=>{let i=this.eventId(),e=Up(this.event);if(this.request?.unsubscribe(),this.detail.set(null),this.activeTab.set(`payload`),this.showDiff.set(!1),!e){this.loading.set(!1);return}let t=Date.now();this.loading.set(!0);let o=!0;this.request=this.svc.getEventDetail(e.aggregateType,e.aggregateId,e.sequence).pipe(_o(this.destroyRef),Vi$1(r=>(this.messageService.add({severity:`error`,summary:`Error`,detail:Lo(r,`Failed to load the event details.`)}),this.finishLoading(),ht$1))).subscribe(r=>{this.detail.set(W(G$1({},r),{stateKnown:r.stateKnown??!0,previousStateKnown:r.previousStateKnown??!0})),o?this.finishLoading():Io(t,()=>{this.eventId()===i&&this.finishLoading()})}),o=!1})}copy(i,e){Qt(i).then(t=>{t?(this.copiedKey.set(e),setTimeout(()=>this.copiedKey.set(null),1500)):this.messageService.add({severity:`warn`,summary:`Copy failed`,detail:`Your browser blocked copying. Select the text and copy it manually.`})})}finishLoading(){this.loading.set(!1),this.loaded.emit()}static ɵfac=function(e){return new(e||n)};static ɵcmp=II({type:n,selectors:[[`app-event-detail`]],inputs:{event:[1,`event`]},outputs:{loaded:`loaded`},decls:2,vars:1,consts:[[1,`pb-5`,`border-b`,`border-surface-100`,`dark:border-surface-800`],[1,`flex`,`items-baseline`,`gap-2`],[1,`font-mono`,`text-sm`,`tabular-nums`,`text-surface-400`],[1,`font-semibold`,`text-base`,`text-surface-900`,`dark:text-surface-100`],[`severity`,`secondary`,`styleClass`,`shrink-0`,1,`self-center`,3,`value`],[1,`text-xs`,`text-surface-400`,`mt-0.5`],[1,`py-4`,`flex`,`flex-col`,`gap-3`],[1,`flex`,`items-center`,`justify-between`,`bg-surface-50`,`dark:bg-surface-800`,`rounded-lg`,`px-3`,`py-2.5`],[1,`text-xs`,`font-medium`,`text-surface-400`,`uppercase`,`tracking-widest`,`mb-1`],[1,`text-sm`,`font-mono`,`text-surface-900`,`dark:text-surface-100`],[`severity`,`secondary`,`tooltipPosition`,`top`,3,`onClick`,`text`,`rounded`,`icon`,`pTooltip`],[1,`border-t`,`border-surface-100`,`dark:border-surface-800`,`pt-4`],[3,`valueChange`,`value`],[`styleClass`,`!border-t-0`],[`value`,`payload`],[`value`,`metadata`],[`value`,`state`],[1,`flex`,`items-center`,`gap-1.5`],[`severity`,`secondary`,3,`value`],[1,`pt-4`],[1,`relative`,`group`,`mb-6`],[1,`bg-surface-100`,`dark:bg-surface-800`,`rounded-lg`,`px-4`,`py-3`,`text-xs`,`leading-relaxed`,`overflow-auto`,`max-h-72`,`font-mono`,3,`innerHTML`],[`severity`,`secondary`,`tooltipPosition`,`top`,1,`!absolute`,`top-1.5`,`right-1.5`,`opacity-0`,`group-hover:opacity-100`,`transition-opacity`,3,`onClick`,`text`,`rounded`,`icon`,`pTooltip`],[1,`rounded-lg`,`border`,`border-surface-100`,`dark:border-surface-800`,`overflow-hidden`],[1,`text-sm`,`text-surface-400`],[1,`w-full`,`text-xs`],[1,`bg-surface-50`,`dark:bg-surface-700`,`border-b`,`border-surface-100`,`dark:border-surface-800`],[1,`text-left`,`px-4`,`py-2`,`text-surface-400`,`font-medium`,`uppercase`,`tracking-widest`,`w-2/5`],[1,`text-left`,`px-4`,`py-2`,`text-surface-400`,`font-medium`,`uppercase`,`tracking-widest`],[1,`group`,`border-b`,`border-surface-100`,`dark:border-surface-800`,`last:border-0`,`hover:bg-surface-50`,`dark:hover:bg-surface-800`],[1,`px-4`,`py-2.5`,`font-mono`,`text-primary-600`,`dark:text-primary-400`,`break-all`],[1,`px-4`,`py-2.5`,`text-surface-900`,`dark:text-surface-100`,`break-all`],[1,`flex`,`items-center`,`justify-between`,`gap-2`],[1,`break-all`],[`severity`,`secondary`,`tooltipPosition`,`top`,1,`opacity-0`,`group-hover:opacity-100`,`transition-opacity`,3,`onClick`,`text`,`rounded`,`icon`,`pTooltip`],[1,`relative`,`group`],[1,`bg-surface-100`,`dark:bg-surface-800`,`text-surface-400`,`rounded-lg`,`px-4`,`py-3`,`text-xs`,`leading-relaxed`,`overflow-auto`,`max-h-72`,`font-mono`],[1,`absolute`,`top-1.5`,`right-1.5`,`flex`,`items-center`,`gap-1`,`opacity-0`,`group-hover:opacity-100`,`transition-opacity`],[`icon`,`pi pi-arrow-right-arrow-left`,`size`,`small`,`styleClass`,`!w-[6.5rem]`,3,`text`,`rounded`,`severity`,`label`],[`severity`,`secondary`,`tooltipPosition`,`top`,3,`text`,`rounded`,`icon`,`pTooltip`],[1,`mt-2`,`text-xs`,`text-surface-400`],[`icon`,`pi pi-arrow-right-arrow-left`,`size`,`small`,`styleClass`,`!w-[6.5rem]`,3,`onClick`,`text`,`rounded`,`severity`,`label`]],template:function(e,t){if(e&1&&qI(0,oa,1,0,`app-detail-skeleton`)(1,xa,40,22),e&2){let o;GI(t.loading()?0:(o=t.detail())?1:-1,o)}},dependencies:[Ji,fn,Wt,mt,zt,$e,Je,ht,Eo,Do,Gt,Vs$1,qt,en],encapsulation:2})};var _a=[`*`];function wa(n,i){if(n&1&&ip(0,`span`,3),n&2)xE(aE().upperLineClass())}function Ca(n,i){if(n&1&&ip(0,`span`,4),n&2)xE(aE().lowerLineClass())}var tn=class n{tone=mL(`neutral`);selected=mL(!1);reached=mL(!1);interactive=mL(!0);first=mL(!1);last=mL(!1);rowClass=sD(()=>this.selected()?`cursor-pointer bg-primary-50 dark:bg-primary-950 shadow-[inset_2px_0_0_var(--p-primary-500)]`:this.interactive()?`cursor-pointer hover:bg-surface-50 dark:hover:bg-surface-800`:``);filledLine=`bg-primary-500`;emptyLine=`bg-surface-200 dark:bg-surface-700`;upperLineClass=sD(()=>this.reached()&&!this.selected()?this.filledLine:this.emptyLine);lowerLineClass=sD(()=>this.reached()?this.filledLine:this.emptyLine);dotClass=sD(()=>{let[i,e]={neutral:this.reached()||this.selected()?[`bg-primary-500`,`ring-primary-500/20`]:[`bg-surface-300 dark:bg-surface-600`,``],success:[`bg-emerald-500`,`ring-emerald-500/20`],failure:[`bg-red-500`,`ring-red-500/20`],placeholder:[`bg-surface-200 dark:bg-surface-700`,``]}[this.tone()];return`${i} ${this.selected()?`border-primary-50 dark:border-primary-950 scale-125 ring-4 ${e}`:this.interactive()?`border-surface-0 dark:border-surface-900 group-hover:border-surface-50 dark:group-hover:border-surface-800`:`border-surface-0 dark:border-surface-900`}`});static ɵfac=function(e){return new(e||n)};static ɵcmp=II({type:n,selectors:[[`app-timeline-item`]],hostAttrs:[1,`group`,`relative`,`flex`,`items-center`,`gap-3`,`pl-[42px]`,`pr-4`,`py-3`,`transition-colors`],hostVars:2,hostBindings:function(e,t){e&2&&xE(t.rowClass())},inputs:{tone:[1,`tone`],selected:[1,`selected`],reached:[1,`reached`],interactive:[1,`interactive`],first:[1,`first`],last:[1,`last`]},ngContentSelectors:_a,decls:4,vars:4,consts:[[1,`absolute`,`left-[23px]`,`top-0`,`bottom-1/2`,`w-px`,`transition-colors`,`duration-300`,3,`class`],[1,`absolute`,`left-[23px]`,`top-1/2`,`bottom-0`,`w-px`,`transition-colors`,`duration-300`,3,`class`],[1,`absolute`,`left-[16px]`,`top-1/2`,`-translate-y-1/2`,`h-[15px]`,`w-[15px]`,`rounded-full`,`border-[3px]`,`transition-all`,`duration-300`],[1,`absolute`,`left-[23px]`,`top-0`,`bottom-1/2`,`w-px`,`transition-colors`,`duration-300`],[1,`absolute`,`left-[23px]`,`top-1/2`,`bottom-0`,`w-px`,`transition-colors`,`duration-300`]],template:function(e,t){e&1&&(lE(),qI(0,wa,1,2,`span`,0),qI(1,Ca,1,2,`span`,1),ip(2,`span`,2),uE(3)),e&2&&(GI(t.first()?-1:0),Zy(),GI(t.last()?-1:1),Zy(),xE(t.dotClass()))},encapsulation:2})};var Ta=(n,i)=>i.id;function Da(n,i){if(n&1&&op(0,`p-tag`,25),n&2){let e=aE().$implicit;rp(`value`,`rev `+e.revision)}}function Ea(n,i){if(n&1&&(ni(0,`app-timeline-item`,17)(1,`div`,20)(2,`div`,21)(3,`span`,22)(4,`span`,23),HE(5),gc(),ni(6,`span`,24),HE(7),gc()(),qI(8,Da,1,1,`p-tag`,25),gc(),ni(9,`span`,26),HE(10),JE(11,`date`),gc()(),op(12,`i`,27),gc()),n&2){let e=i.$implicit,t=i.$index,o=i.$index,r=i.$count,s=aE();rp(`selected`,e===s.selected)(`reached`,t>=s.events.indexOf(s.selected))(`first`,o===0)(`last`,o===r-1)(`interactive`,!1),Zy(5),Ec(`#`,e.sequence),Zy(2),Sp(e.type),Zy(),GI(e.revision>1?8:-1),Zy(2),Sp(tD(11,9,e.timestamp,`MMM d, y · HH:mm:ss`))}}var vt=`order-7f3a`;var Sa=Date.now();var ka=(n,i=0)=>new Date(Sa-n*6e4+i).toISOString();var Ma=[{command:`PlaceOrder`,minutesAgo:190,events:[{type:`OrderPlaced`,payload:{customerId:`customer-42`,items:[{sku:`CHAIR-OAK`,quantity:2,unitPrice:64.95},{sku:`LAMP-BRASS`,quantity:1,unitPrice:39.5}],total:169.4},apply:(n,i)=>({id:vt,status:`PLACED`,customerId:`customer-42`,total:169.4,placedAt:i})},{type:`OrderConfirmed`,payload:{paymentReference:`PSP-7F3A-92K1`},apply:(n,i)=>W(G$1({},n),{status:`CONFIRMED`,paymentReference:`PSP-7F3A-92K1`,confirmedAt:i})}]},{command:`ShipOrder`,minutesAgo:80,events:[{type:`OrderShipped`,revision:2,payload:{carrier:`DHL`,trackingNumber:`JD014600003SE`},apply:(n,i)=>W(G$1({},n),{status:`SHIPPED`,carrier:`DHL`,trackingNumber:`JD014600003SE`,shippedAt:i})}]},{command:`DeliverOrder`,minutesAgo:12,events:[{type:`OrderDelivered`,payload:{signedBy:`J. de Vries`},apply:(n,i)=>W(G$1({},n),{status:`DELIVERED`,signedBy:`J. de Vries`,deliveredAt:i})}]}];function Ia(){let n=[],i=new Map,e={},t=0;return Ma.forEach((o,r)=>{let s=`5f0c2b1e-7d4a-4c8e-9b3f-${String(r+1).padStart(12,`0`)}`;o.events.forEach((a,d)=>{let c={id:`8c1d4e2a-3b5f-4a6c-9d7e-${String(++t).padStart(12,`0`)}`,sequence:t,timestamp:ka(o.minutesAgo,(d+1)*4),type:a.type,aggregateType:`order`,aggregateId:vt,revision:a.revision??1,payload:G$1({id:vt},a.payload),metadata:{$correlationId:s}};e=a.apply(e,c.timestamp),n.push(c),i.set(c.sequence,{aggregateId:vt,type:`Order`,version:c.sequence,timestamp:c.timestamp,metadata:{},payload:e})})}),{events:n.reverse(),states:i}}var yt=Ia();var La={getEventDetail:(n,i,e)=>{return Ch({event:yt.events.find(o=>o.sequence===e),state:yt.states.get(e)??null,previousState:yt.states.get(e-1)??null,stateKnown:!0,previousStateKnown:!0})}};var Uo=class n{orderId=vt;events=yt.events;selected=yt.events[1];eventDetail=vL(nt);showStateDiff(){this.eventDetail()?.activeTab.set(`state`),this.eventDetail()?.showDiff.set(!0)}static ɵfac=function(e){return new(e||n)};static ɵcmp=II({type:n,selectors:[[`app-console-screen`]],viewQuery:function(e,t){e&1&&gp(t.eventDetail,nt,5),e&2&&hE()},hostAttrs:[`inert`,``,`aria-hidden`,`true`],features:[QE([yd,{provide:Ye,useValue:La}])],decls:27,vars:2,consts:[[1,`pointer-events-none`,`overflow-hidden`,`rounded-xl`,`border`,`border-surface-200`,`bg-surface-0`,`text-left`,`shadow-[0_40px_100px_-40px_rgba(15,23,42,0.45)]`,`select-none`,`dark:border-surface-700`,`dark:bg-surface-900`],[1,`flex`,`items-center`,`gap-3`,`border-b`,`border-slate-700`,`bg-slate-900`,`px-5`,`py-3`],[1,`pi`,`pi-bolt`,`text-xl`,`text-primary-400`],[1,`text-lg`,`tracking-tight`],[1,`font-semibold`,`text-white`],[1,`ml-1.5`,`font-light`,`text-slate-300`],[1,`relative`,`ml-auto`,`w-80`],[1,`pi`,`pi-search`,`absolute`,`top-1/2`,`left-2.5`,`-translate-y-1/2`,`text-xs`,`text-slate-400`],[1,`flex`,`h-8`,`items-center`,`rounded`,`border`,`border-slate-700`,`bg-slate-800`,`pl-8`,`text-sm`,`text-slate-100`],[1,`flex`,`h-[34rem]`],[1,`flex`,`w-[340px]`,`shrink-0`,`flex-col`,`border-r`,`border-surface-100`,`dark:border-surface-800`],[`value`,`events`,1,`!bg-transparent`],[1,`px-4`],[`value`,`events`],[`value`,`commands`],[1,`min-h-0`,`flex-1`,`px-4`,`pt-3`,`pb-4`],[1,`overflow-hidden`,`rounded-lg`,`border`,`border-surface-100`,`dark:border-surface-800`],[3,`selected`,`reached`,`first`,`last`,`interactive`],[1,`flex-1`,`overflow-hidden`,`px-6`,`py-5`],[3,`loaded`,`event`],[1,`flex`,`min-w-0`,`flex-1`,`flex-col`],[1,`flex`,`h-[22px]`,`items-center`,`gap-2`],[1,`flex`,`min-w-0`,`items-baseline`,`gap-2`],[1,`shrink-0`,`font-mono`,`text-xs`,`tabular-nums`,`text-surface-400`],[1,`truncate`,`text-sm`,`font-medium`],[`severity`,`secondary`,`styleClass`,`shrink-0`,3,`value`],[1,`mt-0.5`,`text-xs`,`text-surface-400`],[1,`pi`,`pi-chevron-right`,`shrink-0`,`text-sm`,`text-surface-400`]],template:function(e,t){e&1&&(ni(0,`div`,0)(1,`div`,1),op(2,`i`,2),ni(3,`span`,3)(4,`span`,4),HE(5,`Eventify`),gc(),ni(6,`span`,5),HE(7,`Console`),gc()(),ni(8,`div`,6),op(9,`i`,7),ni(10,`div`,8),HE(11),gc()()(),ni(12,`div`,9)(13,`div`,10)(14,`p-tabs`,11)(15,`div`,12)(16,`p-tablist`)(17,`p-tab`,13),HE(18,`Events`),gc(),ni(19,`p-tab`,14),HE(20,`Commands`),gc()()()(),ni(21,`div`,15)(22,`div`,16),zI(23,Ea,13,12,`app-timeline-item`,17,Ta),gc()()(),ni(25,`div`,18)(26,`app-event-detail`,19),up(`loaded`,function(){return t.showStateDiff()}),gc()()()()),e&2&&(Zy(11),Sp(t.orderId),Zy(12),QI(t.events),Zy(3),rp(`event`,t.selected))},dependencies:[zt,$e,Je,ht,Wt,mt,tn,nt,Vs$1],encapsulation:2})};var $a=new Set([`public`,`private`,`class`,`interface`,`return`,`new`,`if`,`else`,`throw`,`null`,`void`,`final`,`import`,`static`]);var Aa=/(\/\/[^\n]*)|("(?:[^"\\]|\\.)*")|(@\w+)|\b([A-Za-z_]\w*)\b/g;function Ko(n){return n.replace(/&/g,`&amp;`).replace(/</g,`&lt;`).replace(/>/g,`&gt;`).replace(Aa,(e,t,o,r,s)=>t?`<span class="text-slate-500 italic">${t}</span>`:o?`<span class="text-emerald-300">${o}</span>`:r?`<span class="text-amber-300">${r}</span>`:$a.has(s)?`<span class="text-sky-300">${s}</span>`:/^[A-Z]/.test(s)?`<span class="text-primary-300">${s}</span>`:e)}var Oa=(n,i)=>i.title;function Fa(n,i){n&1&&ip(0,`span`,2)}function Ba(n,i){if(n&1){let e=eE();mc(0,`li`,1),qI(1,Fa,1,0,`span`,2),mc(2,`span`,3),HE(3),yc(),mc(4,`div`,4)(5,`h3`,5),HE(6),yc(),ip(7,`p`,6),yc(),mc(8,`div`,7)(9,`div`,8)(10,`span`,9),HE(11),yc(),mc(12,`button`,10),dp(`click`,function(){let o=ql(e).$index;return Gl(aE().copy(o))}),ip(13,`i`,11),mc(14,`span`,12),HE(15),yc()()(),mc(16,`pre`,13),ip(17,`code`,14),yc()()()}if(n&2){let e=i.$implicit,t=i.$index,o=i.$count,r=aE();Dp(`pb-10`,t!==o-1),Zy(),GI(t!==o-1?1:-1),Zy(2),Sp(t+1),Zy(3),Sp(e.title),Zy(),cp(`innerHTML`,e.text,ay),Zy(4),Sp(e.label),Zy(),np(`aria-label`,r.copied()===t?`Copied`:`Copy `+e.label),Zy(),xE(r.copied()===t?`pi-check text-primary-400`:`pi-copy`),Zy(2),Sp(r.copied()===t?`Copied`:`Copy`),Zy(2),cp(`innerHTML`,e.html,ay)}}var Go=n=>n.replace(/&/g,`&amp;`).replace(/</g,`&lt;`).replace(/>/g,`&gt;`);var Pa=n=>Go(n).replace(/(&lt;\/?[\w.-]+&gt;)/g,`<span class="text-sky-300">$1</span>`);var Ra=n=>Go(n).replace(/^(docker run)/,`<span class="text-primary-300">$1</span>`).replace(/(\s-[\w-]+)/g,`<span class="text-slate-400">$1</span>`);var ja={shell:Ra,xml:Pa,java:Ko};var qo=class n{steps=mL.required();highlighted=sD(()=>this.steps().map(i=>W(G$1({},i),{html:ja[i.language](i.code)})));copied=_o$1(null);resetCopied;constructor(){C(Ce).onDestroy(()=>clearTimeout(this.resetCopied))}async copy(i){await Qt(this.steps()[i].code)&&(this.copied.set(i),clearTimeout(this.resetCopied),this.resetCopied=setTimeout(()=>this.copied.set(null),2e3))}static ɵfac=function(e){return new(e||n)};static ɵcmp=II({type:n,selectors:[[`app-get-started`]],inputs:{steps:[1,`steps`]},decls:3,vars:0,consts:[[1,`relative`,`grid`,`grid-cols-[2rem_minmax(0,1fr)]`,`gap-x-4`,`lg:grid-cols-[2rem_minmax(0,1fr)_minmax(0,1.7fr)]`,`lg:gap-x-8`,3,`pb-10`],[1,`relative`,`grid`,`grid-cols-[2rem_minmax(0,1fr)]`,`gap-x-4`,`lg:grid-cols-[2rem_minmax(0,1fr)_minmax(0,1.7fr)]`,`lg:gap-x-8`],[`aria-hidden`,`true`,1,`absolute`,`top-9`,`bottom-1`,`left-[calc(1rem-0.5px)]`,`w-px`,`bg-linear-to-b`,`from-primary-300`,`to-surface-200`,`dark:from-primary-700`,`dark:to-surface-700`],[1,`flex`,`h-8`,`w-8`,`items-center`,`justify-center`,`rounded-full`,`border`,`border-primary-200`,`bg-primary-50`,`text-sm`,`font-semibold`,`text-primary-700`,`dark:border-primary-800`,`dark:bg-primary-950`,`dark:text-primary-300`],[1,`min-w-0`],[1,`pt-1`,`text-base`,`font-semibold`,`text-surface-900`,`dark:text-surface-0`],[1,`step-text`,`mt-1`,`text-sm`,`leading-relaxed`,`text-surface-500`,`dark:text-surface-400`,3,`innerHTML`],[1,`col-start-2`,`mt-4`,`min-w-0`,`overflow-hidden`,`rounded-xl`,`border`,`border-slate-800`,`bg-slate-900`,`shadow-[0_20px_50px_-30px_rgba(15,23,42,0.6)]`,`transition-transform`,`duration-500`,`hover:-translate-y-0.5`,`lg:col-start-3`,`lg:mt-0`],[1,`flex`,`items-center`,`justify-between`,`border-b`,`border-white/10`,`py-1.5`,`pr-1.5`,`pl-4`],[1,`text-xs`,`font-medium`,`text-slate-400`],[`type`,`button`,1,`inline-flex`,`cursor-pointer`,`items-center`,`gap-1.5`,`rounded-md`,`px-2`,`py-1`,`text-xs`,`text-slate-400`,`transition-colors`,`hover:bg-white/5`,`hover:text-slate-100`,3,`click`],[1,`pi`,`text-xs`],[`aria-live`,`polite`],[1,`overflow-x-auto`,`px-4`,`py-3.5`,`font-mono`,`text-[0.8rem]`,`leading-relaxed`,`text-slate-200`],[3,`innerHTML`]],template:function(e,t){e&1&&(mc(0,`ol`),zI(1,Ba,18,12,`li`,0,Oa),yc()),e&2&&(Zy(),QI(t.highlighted()))},styles:[`.step-text[_ngcontent-%COMP%]     code{font-family:var(--%NS%font-mono);font-size:.8rem;color:var(--%NS%p-surface-700)}@media(prefers-color-scheme:dark){.step-text[_ngcontent-%COMP%]     code{color:var(--%NS%p-surface-200)}}`]})};var Qo=class n{static ɵfac=function(e){return new(e||n)};static ɵcmp=II({type:n,selectors:[[`app-site-footer`]],decls:13,vars:0,consts:[[1,`border-t`,`border-white/10`,`bg-slate-950`],[1,`mx-auto`,`flex`,`max-w-6xl`,`flex-col`,`gap-4`,`px-6`,`py-7`,`text-sm`,`text-slate-400`,`sm:flex-row`,`sm:items-center`,`sm:justify-between`],[1,`flex`,`items-center`,`gap-2`],[1,`pi`,`pi-bolt`,`text-primary-400`],[`aria-label`,`Footer links`,1,`flex`,`items-center`,`gap-5`],[`href`,`https://alikelleci.github.io/eventify/docs/`,`target`,`_blank`,`rel`,`noopener`,1,`hover:text-white`],[`href`,`https://github.com/alikelleci/eventify`,`target`,`_blank`,`rel`,`noopener`,1,`hover:text-white`],[`href`,`https://github.com/alikelleci/eventify/blob/main/LICENSE`,`target`,`_blank`,`rel`,`noopener`,1,`hover:text-white`]],template:function(e,t){e&1&&(mc(0,`footer`,0)(1,`div`,1)(2,`div`,2),ip(3,`i`,3),mc(4,`span`),HE(5,`Eventify · Event sourcing for Java`),yc()(),mc(6,`nav`,4)(7,`a`,5),HE(8,`Docs`),yc(),mc(9,`a`,6),HE(10,`GitHub`),yc(),mc(11,`a`,7),HE(12,`Apache 2.0`),yc()()()())},encapsulation:2})};var Zo=class{constructor(i){this.frames=i;this.frame.set(i[0].length-1)}frames;step=_o$1(0);frame=_o$1(0);playing=_o$1(!1);run=_o$1(0);timer;reducedMotion=window.matchMedia(`(prefers-reduced-motion: reduce)`).matches;pausedByViewer=!1;duration(i){return this.frames[i].reduce((e,t)=>e+t,0)}go(i){clearTimeout(this.timer),this.step.set(i),this.run.update(e=>e+1),this.playing()?(this.frame.set(0),this.schedule()):this.frame.set(this.frames[i].length-1)}play(){this.reducedMotion||(this.playing.set(!0),this.go(this.step()))}pause(){clearTimeout(this.timer),this.playing.set(!1)}toggle(){this.pausedByViewer=this.playing(),this.playing()?this.pause():this.play()}inView(i){i&&!this.playing()&&!this.pausedByViewer&&this.play(),!i&&this.playing()&&this.pause()}destroy(){clearTimeout(this.timer)}schedule(){this.timer=setTimeout(()=>{this.frame()<this.frames[this.step()].length-1?(this.frame.update(i=>i+1),this.schedule()):this.go((this.step()+1)%this.frames.length)},this.frames[this.step()][this.frame()])}};function qp(n,i=.4){let e=!1,t=()=>n.inView(e&&!document.hidden),o=new IntersectionObserver(([r])=>{e=r.isIntersecting,t()},{threshold:i});o.observe(C(dr$1).nativeElement),document.addEventListener(`visibilitychange`,t),C(Ce).onDestroy(()=>{o.disconnect(),document.removeEventListener(`visibilitychange`,t),n.destroy()})}export{Zo as a,qp as c,Uo as i,Ko as n,fn as o,Qo as r,qo as s,Ji as t};