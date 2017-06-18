class ComicItemView extends React.Component {
	constructor(props) {
		super(props);
		
	}
		
	render() {
		return (
			<div className="index-comic tooltip">
				<span className="name">{this.props.name}</span>
				<a href={"/comic/" + this.props.id} >
					<img src={this.props.coverUrl}></img>
					<span className="description tooltiptext">{this.props.description}</span>
				</a>
			</div>
		);
	}
}

class NavigatorView extends React.Component {
	constructor(props) {
		super(props);
		
		YoedgeUtils.bindClass(this, NavigatorView);
	}
	
	goToPage(pageNo) {
		this.props.goToPage(pageNo);
	}
	
	render() {
		let currentPageNo = this.props.currentPageNo;
		let endPageNo = this.props.endPageNo;
		let fromPageNo = Math.max(1, currentPageNo - 5);
		let toPageNo = Math.min(endPageNo, currentPageNo + 5);
		
		let isFirstPage = currentPageNo <= 1;
		let isLastPage = currentPageNo >= endPageNo;
		let preCls = isFirstPage ? "txt disabled" : "txt";
		let nextCls = isLastPage ? "txt disabled" : "txt";
		
		let pageIdxViews = [];
		if (!isFirstPage) pageIdxViews.push(<span>...</span>);
		for (let pageNo = fromPageNo; pageNo <= toPageNo; pageNo++) {
			let clsName = pageNo == currentPageNo ? "txt selected" : "txt";
			let newView = (<a href="#" className={clsName} onClick={this.goToPage.bind(this, pageNo)}>{pageNo}</a>);
			pageIdxViews.push(newView);
		}
		if (!isLastPage) pageIdxViews.push(<span>...</span>);
		
		return (
			<div className="index-navigator">
				<a href="#" className={preCls} onClick={this.goToPage.bind(this, 1)}>首页</a>
				<a href="#" className={preCls} onClick={this.goToPage.bind(this, currentPageNo - 1)}>上一页</a>
				{pageIdxViews}
				<a href="#" className={nextCls} onClick={this.goToPage.bind(this, currentPageNo + 1)}>下一页</a>
				<a href="#" className={nextCls} onClick={this.goToPage.bind(this, endPageNo)}>末页</a>
			</div>
		);
	}
}

let PAGE_SIZE = 20;
class ComicsView extends React.Component {
	constructor(props) {
		super(props);
		YoedgeUtils.bindClass(this, ComicsView);
		
		this.state = {currentPageNo:1}
		
		this.totalCount = 0;
		this.pages = {}; // key:pageNo, value: comics
		this.targetPageNo = 1;
	}
	
	queryPage(pageNo) {		
		$.ajax ({
			url: `/api/comics?page=${pageNo}&size=${PAGE_SIZE}`,
			success: resp => {
				if (!resp.success) return;	
				this.addPageData(resp.totalCount, resp.pageNo, resp.comics);
			}
		})
	}
	
	goToPage(pageNo) {
		if (this.pages[pageNo]) {
			this.setState({currentPageNo:pageNo});
			return;
		}
		
		if (pageNo < 1 || pageNo > this.calcEndPageNo()) return;
		if (pageNo == this.state.currentPageNo) return;
		
		this.targetPageNo = pageNo;
		this.queryPage(pageNo)
	}
	
	addPageData(totalCount, pageNo, comics = []) {
		this.totalCount = totalCount;
		this.pages[pageNo] = comics;
		this.setState({currentPageNo:this.targetPageNo})
	}
	
	calcEndPageNo() {
		return Math.ceil(this.totalCount / PAGE_SIZE);
	}
	
	render() {
		let comicViews = [];		
		let comics = this.pages[this.state.currentPageNo] || [];		
		comics.forEach((comic)=>{
			let newView = (<ComicItemView {...comic}/>);
			comicViews.push(newView);
		})
		
		let endPageNo = this.calcEndPageNo();
		let navigatorParams = {endPageNo:endPageNo, currentPageNo:this.state.currentPageNo, goToPage:this.goToPage}
		
		return (
			<div className="index-root">
				<div className="index-comics">
					{comicViews}
				</div>
				<NavigatorView ref="navigator"  {...navigatorParams}/>
			</div>
		);
	}
}

let comicsView = ReactDOM.render(<ComicsView />, document.getElementById('container-comics'));
//comicsView.addPageData(1230, 10, comics)
comicsView.queryPage(1);

window.ComicsView = comicsView;

