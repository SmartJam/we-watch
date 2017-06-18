

let bindThis = (context,...keys) => {
    keys.forEach((k) =>{
		if(k === 'constructor' || k === 'render') return;		//skip this two methods
        try {
            context[k] = context[k].bind(context)
        }catch(t){
            //ignore
            console.log(t,k,context)
        }
    })
}

let bindClass = (context, clazz) => {
		bindThis.apply(null,[context].concat(Object.getOwnPropertyNames(clazz.prototype )));
}

let YoedgeUtils = {
	bindThis,
	bindClass,
}

window.YoedgeUtils = YoedgeUtils